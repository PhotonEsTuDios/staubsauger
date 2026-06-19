"""
PhotonEsTuDios — Dropbox-Ingest LOKAL (Test/Sofortlauf).

Zieht Fotos von Dropbox -> S3 + DynamoDB, identische Dedup-Logik wie die
Cloud-Lambda (handler.py), laeuft aber auf Danielas Rechner mit dem
vorhandenen AWS-Profil `photon` (S3 + DynamoDB). Braucht KEIN Secrets Manager,
KEIN Lambda, KEINE IAM-Rolle.

Nimmt ein KURZLEBIGES Dropbox-Access-Token (aus dem App-Console "Generate
access token"), gelesen aus einer Datei -- das Token wird nie ausgegeben.

  AWS_PROFILE=photon python3 local_ingest.py --token-file ~/Desktop/appkey [--dry-run] [--limit N] [--folder /Pfad]

Fuer den Dauerbetrieb in der Cloud spaeter: Refresh-Token + Lambda (Rainer).
"""
import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request

import boto3

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff",
              ".nef", ".cr2", ".cr3", ".arw", ".dng", ".raf",
              ".mov", ".mp4", ".m4v"}

_ddb = boto3.resource("dynamodb", region_name="eu-west-1")
_table = _ddb.Table(TABLE)
_s3 = boto3.client("s3", region_name="eu-west-1")


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token-file", required=True)
    ap.add_argument("--folder", default="", help='Dropbox-Ordner, "" = alles')
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="max. neue Uploads (0 = unbegrenzt)")
    args = ap.parse_args()

    with open(os.path.expanduser(args.token_file)) as f:
        token = f.read().strip()
    if not token:
        sys.exit("Token-Datei ist leer.")

    try:
        page = _rpc(token, "files/list_folder", {"path": args.folder, "recursive": True})
    except urllib.error.HTTPError as e:
        sys.exit(f"Dropbox-Fehler ({e.code}): {e.read().decode(errors='replace')}")

    uploaded = skipped = scanned = 0
    while True:
        for entry in page.get("entries", []):
            if entry.get(".tag") != "file":
                continue
            name = entry["name"]
            ext = os.path.splitext(name)[1].lower()
            if ext not in IMAGE_EXTS:
                continue
            scanned += 1

            blob = _download(token, entry["path_lower"])
            sha = hashlib.sha256(blob).hexdigest()
            if _table.get_item(Key={"sha256": sha}).get("Item"):
                skipped += 1
                continue

            key = f"{sha}{ext}"
            if args.dry_run:
                print(f"[DRY] wuerde laden: {entry['path_display']}  -> {key}  ({entry.get('size')} B)")
                uploaded += 1
            else:
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
                    "dropbox_rev": entry.get("rev"),
                    "uploaded_at": entry.get("server_modified"),
                })
                print(f"[OK]  {entry['path_display']}  -> {key}")
                uploaded += 1

            if args.limit and uploaded >= args.limit:
                print(f"\nLimit {args.limit} erreicht.")
                print(f"gescannt={scanned} neu={uploaded} uebersprungen={skipped}")
                return

        if not page.get("has_more"):
            break
        page = _rpc(token, "files/list_folder/continue", {"cursor": page["cursor"]})

    print(f"\nFertig. gescannt={scanned} neu={uploaded} uebersprungen={skipped}")


if __name__ == "__main__":
    main()
