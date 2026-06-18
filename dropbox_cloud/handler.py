"""
PhotonEsTuDios — Dropbox-Cloud-Ingest (AWS Lambda, eu-west-1).

Zieht Fotos DIREKT von Dropbox nach S3 + DynamoDB — läuft in der AWS-Cloud,
NICHT über Danielas Rechner. Zeitgesteuert via EventBridge (z.B. stündlich).

Secrets (Dropbox App-Key/-Secret + Refresh-Token) liegen in AWS Secrets Manager
unter dem Namen `photonestudios/dropbox` — NICHT im Repo.

Dedup identisch zur lokalen Pipeline: SHA-256 als S3-Key & DynamoDB-PK.
Inkrementell über einen Dropbox-Cursor (in DynamoDB unter sha256="__dropbox_cursor__").

Status: DEPLOY-FERTIG vorbereitet, aber NOCH NICHT deployt — braucht von Rainer
die Lambda-/Secrets-Manager-/Rolle-Rechte (siehe ../infra/RAINER-cloud-setup.md)
und von Daniela eine Dropbox-App (siehe ../dropbox_cloud/README.md).
"""
import hashlib
import json
import os
import urllib.parse
import urllib.request

import boto3

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
SECRET_NAME = os.environ.get("PHOTON_DROPBOX_SECRET", "photonestudios/dropbox")
DROPBOX_FOLDER = os.environ.get("PHOTON_DROPBOX_FOLDER", "")  # "" = gesamtes App-/Root-Verzeichnis
CURSOR_PK = "__dropbox_cursor__"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff",
              ".nef", ".cr2", ".cr3", ".arw", ".dng", ".raf",
              ".mov", ".mp4", ".m4v"}

_ddb = boto3.resource("dynamodb")
_table = _ddb.Table(TABLE)
_s3 = boto3.client("s3")
_sm = boto3.client("secretsmanager")


def _secret():
    raw = _sm.get_secret_value(SecretId=SECRET_NAME)["SecretString"]
    return json.loads(raw)  # {app_key, app_secret, refresh_token}


def _access_token(sec):
    """Kurzlebiges Access-Token aus dem Refresh-Token holen (OAuth2)."""
    data = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": sec["refresh_token"],
        "client_id": sec["app_key"],
        "client_secret": sec["app_secret"],
    }).encode()
    req = urllib.request.Request("https://api.dropboxapi.com/oauth2/token", data=data)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)["access_token"]


def _rpc(token, endpoint, body):
    req = urllib.request.Request(
        f"https://api.dropboxapi.com/2/{endpoint}",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def _download(token, path):
    req = urllib.request.Request(
        "https://content.dropboxapi.com/2/files/download",
        headers={"Authorization": f"Bearer {token}",
                 "Dropbox-API-Arg": json.dumps({"path": path})},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def _load_cursor():
    item = _table.get_item(Key={"sha256": CURSOR_PK}).get("Item")
    return item.get("cursor") if item else None


def _save_cursor(cursor):
    _table.put_item(Item={"sha256": CURSOR_PK, "cursor": cursor})


def handler(event, context):
    sec = _secret()
    token = _access_token(sec)

    cursor = _load_cursor()
    if cursor:
        page = _rpc(token, "files/list_folder/continue", {"cursor": cursor})
    else:
        page = _rpc(token, "files/list_folder",
                    {"path": DROPBOX_FOLDER, "recursive": True})

    uploaded = skipped = 0
    while True:
        for entry in page.get("entries", []):
            if entry.get(".tag") != "file":
                continue
            name = entry["name"]
            ext = os.path.splitext(name)[1].lower()
            if ext not in IMAGE_EXTS:
                continue

            blob = _download(token, entry["path_lower"])
            sha = hashlib.sha256(blob).hexdigest()
            if _table.get_item(Key={"sha256": sha}).get("Item"):
                skipped += 1
                continue

            key = f"{sha}{ext}"
            _s3.put_object(Bucket=BUCKET, Key=key, Body=blob,
                           Metadata={"original-filename": name, "source": "dropbox"})
            _table.put_item(Item={
                "sha256": sha,
                "s3_bucket": BUCKET,
                "s3_key": key,
                "original_filename": name,
                "source_path": entry["path_display"],
                "source": "dropbox",
                "bytes": entry.get("size"),
                "taken_at": (entry.get("media_info", {})
                             .get("metadata", {}).get("time_taken")),
                "dropbox_rev": entry.get("rev"),
                "uploaded_at": entry.get("server_modified"),
            })
            uploaded += 1

        _save_cursor(page["cursor"])
        if not page.get("has_more"):
            break
        page = _rpc(token, "files/list_folder/continue", {"cursor": page["cursor"]})

    return {"uploaded": uploaded, "skipped": skipped}


if __name__ == "__main__":  # lokaler Smoke-Test (braucht AWS-Profil photon)
    print(handler({}, None))
