#!/usr/bin/env python3
"""
staubsauger.py — PhotonEsTuDios Foto-Staubsauger (lokale Quelle).

Saugt Bilder/Videos aus einem lokalen Pfad flach nach S3 (eu-west-1) und
indexiert jedes Objekt in DynamoDB. Dedup über SHA-256: gleicher Inhalt =
gleicher Key = kein Doppel-Upload, egal aus welcher Quelle.

  S3-Key:        <sha256>.<ext>           (flach, im Bucket-Root)
  DynamoDB-PK:   sha256                    (Inventar: "was ist schon drin")

Quellen-agnostisch: derselbe Index dient später Mac, Festplatte, Dropbox-Cloud.

Beispiele:
  python staubsauger.py ~/Photos/Claude-Workspace --source mac:claude-workspace --dry-run
  python staubsauger.py ~/Photos/Claude-Workspace --source mac:claude-workspace --limit 20
  python staubsauger.py /Volumes/LaCie/Shoots --source drive:lacie --since 2026-01-01
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

# ── feste Stammdaten (PhotonEsTuDios) ──────────────────────────────────────
BUCKET = "photonestudios-photos"
TABLE = "photonestudios-photos"
REGION = "eu-west-1"

IMAGE_EXTS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff", ".webp", ".gif",
    ".nef", ".cr2", ".cr3", ".arw", ".dng", ".raf", ".orf", ".rw2",  # RAW
    ".mov", ".mp4", ".m4v", ".avi",                                   # Video
}

# exiftool-Felder, die wir auslesen (-n = numerisch, GPS als Dezimalgrad)
EXIF_FIELDS = [
    "DateTimeOriginal", "CreateDate", "GPSLatitude", "GPSLongitude",
    "Make", "Model", "LensModel", "LensID", "ISO", "FNumber",
    "ExposureTime", "FocalLength", "ImageWidth", "ImageHeight", "MIMEType",
]


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def exif_to_iso(value):
    """exiftool-Datum 'YYYY:MM:DD HH:MM:SS' -> ISO8601, sonst None."""
    if not value:
        return None
    try:
        clean = str(value).split("+")[0].split(".")[0].strip()
        return dt.datetime.strptime(clean, "%Y:%m:%d %H:%M:%S").isoformat()
    except ValueError:
        return None


def read_metadata(path):
    """Metadaten via exiftool (ein JSON-Objekt). Leeres dict bei Fehler."""
    cmd = ["exiftool", "-j", "-n", "-q"] + [f"-{f}" for f in EXIF_FIELDS] + [path]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
        data = json.loads(out)
        return data[0] if data else {}
    except (subprocess.CalledProcessError, json.JSONDecodeError, IndexError):
        return {}


def num(v):
    """Zahl -> Decimal (DynamoDB-tauglich), sonst None."""
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def build_item(sha, path, source, ext, size, meta):
    taken = exif_to_iso(meta.get("DateTimeOriginal") or meta.get("CreateDate"))
    item = {
        "sha256": sha,
        "s3_bucket": BUCKET,
        "s3_key": f"{sha}{ext}",
        "original_filename": os.path.basename(path),
        "source_path": path,
        "source": source,
        "bytes": num(size),
        "mime": meta.get("MIMEType"),
        "taken_at": taken,
        "ym": taken[:7] if taken else None,          # 'YYYY-MM' für spätere GSI
        "gps_lat": num(meta.get("GPSLatitude")),
        "gps_lon": num(meta.get("GPSLongitude")),
        "camera_make": meta.get("Make"),
        "camera_model": meta.get("Model"),
        "lens": meta.get("LensModel") or meta.get("LensID"),
        "iso": num(meta.get("ISO")),
        "f_number": num(meta.get("FNumber")),
        "exposure": str(meta.get("ExposureTime")) if meta.get("ExposureTime") is not None else None,
        "focal_length": num(meta.get("FocalLength")),
        "width": num(meta.get("ImageWidth")),
        "height": num(meta.get("ImageHeight")),
        "uploaded_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0, tzinfo=None).isoformat() + "Z",
    }
    return {k: v for k, v in item.items() if v is not None}  # leere Felder weglassen


def iter_files(root, exts, since):
    since_ts = (
        dt.datetime.strptime(since, "%Y-%m-%d").timestamp() if since else None
    )
    for dirpath, _, names in os.walk(root):
        for name in names:
            if name.startswith("."):
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext not in exts:
                continue
            full = os.path.join(dirpath, name)
            if since_ts and os.path.getmtime(full) < since_ts:
                continue
            yield full, ext


def main():
    ap = argparse.ArgumentParser(description="PhotonEsTuDios Foto-Staubsauger (lokal)")
    ap.add_argument("path", help="Quell-Ordner (wird rekursiv durchsucht)")
    ap.add_argument("--source", required=True, help="Quellen-Label, z.B. mac:claude-workspace")
    ap.add_argument("--profile", default="photon", help="AWS-Profil (Standard: photon)")
    ap.add_argument("--region", default=REGION)
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--table", default=TABLE)
    ap.add_argument("--since", help="nur Dateien geändert ab Datum YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=0, help="max. N neue Uploads (0 = alle)")
    ap.add_argument("--dry-run", action="store_true", help="nur anzeigen, nichts hochladen")
    args = ap.parse_args()

    if not os.path.isdir(args.path):
        sys.exit(f"Pfad nicht gefunden: {args.path}")

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")
    table = session.resource("dynamodb").Table(args.table)

    seen = uploaded = skipped = errors = 0
    for path, ext in iter_files(args.path, IMAGE_EXTS, args.since):
        seen += 1
        try:
            sha = sha256_of(path)
            # Dedup: schon im Inventar?
            existing = table.get_item(Key={"sha256": sha}).get("Item")
            if existing:
                skipped += 1
                print(f"  skip  {os.path.basename(path)}  (schon drin: {sha[:12]})")
                continue

            size = os.path.getsize(path)
            meta = read_metadata(path)
            item = build_item(sha, path, args.source, ext, size, meta)
            key = item["s3_key"]

            if args.dry_run:
                uploaded += 1
                print(f"  DRY   {os.path.basename(path)} -> s3://{args.bucket}/{key}"
                      f"  [{item.get('taken_at','?')} | {item.get('camera_model','?')}]")
            else:
                s3.upload_file(
                    path, args.bucket, key,
                    ExtraArgs={
                        "ContentType": item.get("mime", "application/octet-stream"),
                        "Metadata": {
                            "original-filename": item["original_filename"],
                            "source": args.source,
                        },
                    },
                )
                # ConditionExpression schützt gegen Races (paralleles Saugen)
                try:
                    table.put_item(Item=item,
                                   ConditionExpression="attribute_not_exists(sha256)")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                        raise
                uploaded += 1
                print(f"  up    {os.path.basename(path)} -> {key[:18]}…"
                      f"  [{item.get('taken_at','?')}]")

            if args.limit and uploaded >= args.limit:
                print(f"  (Limit {args.limit} erreicht)")
                break
        except Exception as e:  # noqa: BLE001 — pro Datei robust weiterlaufen
            errors += 1
            print(f"  ERR   {path}: {e}", file=sys.stderr)

    print(f"\nFertig. gesehen={seen} hochgeladen={uploaded} "
          f"übersprungen={skipped} fehler={errors} "
          f"{'(DRY-RUN)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
