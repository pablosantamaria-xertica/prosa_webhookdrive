import json
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple

from flask import Flask, jsonify, request
from google.api_core.exceptions import Aborted
from google.cloud import firestore
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth import default as google_auth_default

APP = Flask(__name__)

PROJECT_ID = os.getenv("PROJECT_ID", "xertica-presales-data-service")
DOC_ID = os.getenv("DOC_ID", "1AqNcPBqCIan7QsvDwkcMKtM_cp114GpooDZCbc3hrLc")
GCS_BUCKET = os.getenv("GCS_BUCKET", "prosa-artifact")
WEBHOOK_URL = os.getenv(
    "WEBHOOK_URL",
    "https://webhook-drive-843527592623.us-central1.run.app/drive/webhook",
)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")

STATE_COLLECTION = "drive_watch_state"
LOCK_SECONDS = 60

logging.basicConfig(level=logging.INFO)


def _json_log(message: str, **fields: Any) -> None:
    payload = {"message": message, "timestamp": datetime.now(timezone.utc).isoformat()}
    payload.update({k: v for k, v in fields.items() if v is not None})
    logging.info(json.dumps(payload, ensure_ascii=False))


def _firestore_client() -> firestore.Client:
    return firestore.Client(project=PROJECT_ID)


def _storage_client() -> storage.Client:
    return storage.Client(project=PROJECT_ID)


def _drive_service():
    credentials, _ = google_auth_default(scopes=["https://www.googleapis.com/auth/drive"])
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def _state_ref():
    client = _firestore_client()
    return client.collection(STATE_COLLECTION).document(DOC_ID)


def _get_state() -> Dict[str, Any]:
    snapshot = _state_ref().get()
    return snapshot.to_dict() or {}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_admin_token() -> Optional[Tuple[str, int]]:
    token = request.headers.get("X-Admin-Token")
    if not ADMIN_TOKEN or not token or token != ADMIN_TOKEN:
        return jsonify({"error": "unauthorized"}), 401
    return None


def _acquire_lease() -> bool:
    client = _firestore_client()
    ref = _state_ref()
    now = datetime.now(timezone.utc)
    new_lock = now + timedelta(seconds=LOCK_SECONDS)

    @firestore.transactional
    def _txn(transaction: firestore.Transaction) -> bool:
        snapshot = ref.get(transaction=transaction)
        data = snapshot.to_dict() or {}
        locked_until = data.get("lockedUntil")
        if locked_until and locked_until > now:
            return False
        transaction.set(
            ref,
            {"lockedUntil": new_lock, "updatedAt": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        return True

    try:
        return _txn(client.transaction())
    except Aborted:
        return False


def _release_lease() -> None:
    ref = _state_ref()
    ref.set({"lockedUntil": None, "updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)


def _persist_state(update: Dict[str, Any]) -> None:
    update["updatedAt"] = firestore.SERVER_TIMESTAMP
    _state_ref().set(update, merge=True)


def _get_start_page_token(service, state: Dict[str, Any]) -> str:
    token = state.get("startPageToken")
    if token:
        return token
    resp = service.changes().getStartPageToken(supportsAllDrives=True).execute()
    token = resp.get("startPageToken")
    _persist_state({"startPageToken": token})
    return token


def _subscribe_watch() -> Dict[str, Any]:
    service = _drive_service()
    state = _get_state()
    start_page_token = _get_start_page_token(service, state)
    channel_id = str(uuid.uuid4())
    body = {
        "id": channel_id,
        "type": "web_hook",
        "address": WEBHOOK_URL,
    }
    resp = (
        service.changes()
        .watch(pageToken=start_page_token, includeItemsFromAllDrives=True, supportsAllDrives=True, body=body)
        .execute()
    )
    _persist_state(
        {
            "channelId": channel_id,
            "resourceId": resp.get("resourceId"),
            "expiration": resp.get("expiration"),
        }
    )
    return {
        "channelId": channel_id,
        "resourceId": resp.get("resourceId"),
        "expiration": resp.get("expiration"),
        "startPageToken": start_page_token,
    }


def _write_event_to_gcs(event: Dict[str, Any]) -> str:
    storage_client = _storage_client()
    bucket = storage_client.bucket(GCS_BUCKET)
    timestamp = event["receivedAt"].replace(":", "-")
    change_id = event.get("changeId", "unknown")
    object_name = f"docs/{DOC_ID}/events/{timestamp}_{change_id}.json"
    blob = bucket.blob(object_name)
    blob.upload_from_string(json.dumps(event, ensure_ascii=False), content_type="application/json")
    return object_name


def _is_doc_change(change: Dict[str, Any]) -> bool:
    file_id = change.get("fileId")
    if file_id:
        return file_id == DOC_ID
    file_info = change.get("file")
    if file_info and file_info.get("id"):
        return file_info.get("id") == DOC_ID
    return False


def _fetch_file_id(service, change: Dict[str, Any]) -> Optional[str]:
    file_id = change.get("fileId")
    if file_id:
        return file_id
    file_info = change.get("file")
    if file_info and file_info.get("id"):
        return file_info.get("id")
    return None


def _process_changes(resource_context: Dict[str, Any]) -> Dict[str, Any]:
    service = _drive_service()
    state = _get_state()
    start_page_token = _get_start_page_token(service, state)

    message_number = resource_context.get("messageNumber")
    last_message_number = state.get("lastMessageNumber")
    if last_message_number is not None and message_number is not None:
        try:
            if int(message_number) <= int(last_message_number):
                _json_log(
                    "duplicate_message_skipped",
                    docId=DOC_ID,
                    channelId=resource_context.get("channelId"),
                    resourceId=resource_context.get("resourceId"),
                    messageNumber=message_number,
                )
                return {"status": "skipped_duplicate_message"}
        except ValueError:
            pass

    next_page_token = None
    new_start_page_token = None
    processed = 0
    relevant = 0
    last_change_id = state.get("lastProcessedChangeId")

    try:
        page_token = start_page_token
        while page_token:
            resp = (
                service.changes()
                .list(
                    pageToken=page_token,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    fields="nextPageToken,newStartPageToken,changes(fileId,file(id,modifiedTime,version,headRevisionId),changeType,removed,driveId,teamDriveId,driveItemId,kind,changeId)",
                )
                .execute()
            )
            for change in resp.get("changes", []):
                processed += 1
                change_id = change.get("changeId")
                if last_change_id and change_id and str(change_id) == str(last_change_id):
                    continue
                if not _is_doc_change(change):
                    file_id = _fetch_file_id(service, change)
                    if file_id and file_id != DOC_ID:
                        continue
                    if not file_id:
                        continue
                relevant += 1
                file_info = change.get("file", {})
                event = {
                    "docId": DOC_ID,
                    "changeId": change_id,
                    "receivedAt": _iso_now(),
                    "driveModifiedTime": file_info.get("modifiedTime"),
                    "fileVersionOrHeadRevisionId": file_info.get("version") or file_info.get("headRevisionId"),
                    "resourceId": resource_context.get("resourceId"),
                    "channelId": resource_context.get("channelId"),
                    "eventType": change.get("changeType"),
                    "startPageTokenUsed": start_page_token,
                    "newStartPageTokenSaved": None,
                    "nextPageToken": None,
                    "messageNumber": resource_context.get("messageNumber"),
                    "resourceState": resource_context.get("resourceState"),
                    "rawChange": change,
                    "processingStatus": "processed",
                    "errors": None,
                    "snapshotFingerprint": None,
                }
                object_name = _write_event_to_gcs(event)
                _json_log(
                    "change_written",
                    docId=DOC_ID,
                    channelId=resource_context.get("channelId"),
                    resourceId=resource_context.get("resourceId"),
                    messageNumber=resource_context.get("messageNumber"),
                    resourceState=resource_context.get("resourceState"),
                    changeId=change_id,
                    objectName=object_name,
                )
                if change_id:
                    last_change_id = change_id
            page_token = resp.get("nextPageToken")
            next_page_token = page_token
            new_start_page_token = resp.get("newStartPageToken")
        if new_start_page_token:
            _persist_state(
                {
                    "startPageToken": new_start_page_token,
                    "lastProcessedChangeId": last_change_id,
                    "lastMessageNumber": message_number,
                }
            )
    except HttpError as exc:
        if exc.resp.status == 410:
            resp = service.changes().getStartPageToken(supportsAllDrives=True).execute()
            new_token = resp.get("startPageToken")
            _persist_state({"startPageToken": new_token})
            return {"status": "reset_token"}
        raise

    return {
        "status": "ok",
        "processed": processed,
        "relevant": relevant,
        "nextPageToken": next_page_token,
        "newStartPageToken": new_start_page_token,
    }


@APP.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@APP.route("/drive/subscribe", methods=["POST"])
def drive_subscribe():
    auth_error = _require_admin_token()
    if auth_error:
        return auth_error
    payload = _subscribe_watch()
    return jsonify(payload)


@APP.route("/drive/renew", methods=["POST"])
def drive_renew():
    auth_error = _require_admin_token()
    if auth_error:
        return auth_error
    payload = _subscribe_watch()
    return jsonify(payload)


@APP.route("/drive/webhook", methods=["POST"])
def drive_webhook():
    channel_id = request.headers.get("X-Goog-Channel-ID")
    resource_id = request.headers.get("X-Goog-Resource-ID")
    resource_state = request.headers.get("X-Goog-Resource-State")
    message_number = request.headers.get("X-Goog-Message-Number")

    if not channel_id or not resource_id or not resource_state or not message_number:
        return jsonify({"error": "missing headers"}), 400

    state = _get_state()
    if state.get("channelId") != channel_id or state.get("resourceId") != resource_id:
        _json_log(
            "invalid_channel",
            docId=DOC_ID,
            channelId=channel_id,
            resourceId=resource_id,
            messageNumber=message_number,
            resourceState=resource_state,
        )
        return jsonify({"error": "invalid channel"}), 403

    if not _acquire_lease():
        _json_log(
            "lease_busy",
            docId=DOC_ID,
            channelId=channel_id,
            resourceId=resource_id,
            messageNumber=message_number,
            resourceState=resource_state,
        )
        return jsonify({"status": "locked"})

    try:
        context = {
            "channelId": channel_id,
            "resourceId": resource_id,
            "resourceState": resource_state,
            "messageNumber": message_number,
            "requestId": request.headers.get("X-Cloud-Trace-Context"),
        }
        result = _process_changes(context)
        _json_log(
            "webhook_processed",
            docId=DOC_ID,
            channelId=channel_id,
            resourceId=resource_id,
            messageNumber=message_number,
            resourceState=resource_state,
        )
        return jsonify(result)
    finally:
        _release_lease()


if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
