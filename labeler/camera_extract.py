"""PhotonEsTuDios Kamera-Extraktion — liest Kamera (Make/Model), Objektiv und
Aufnahmezeit aus dem EXIF-Header der S3-Originale und schreibt sie ins DynamoDB-Item.
Der Dropbox-Cloud-Ingest hat diese Felder nie geschrieben.

Gleiches Muster wie gps_extract.py: nur die ersten 256 KB per Range-Request, nur Pillow.
NEF/CR2 sind TIFF-basiert, IFD0 mit Make/Model steht am Dateianfang.
Idempotent/resumierbar via exif_version. taken_at wird nur gesetzt, wenn es fehlt oder
NULL ist (der Dropbox-Ingest hat ueberall taken_at=NULL geschrieben).

  AWS_PROFILE=photon python camera_extract.py [--limit N]
"""
import io
import os
import sys
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import boto3
from PIL import Image, ExifTags

REGION = "eu-west-1"
TABLE = "photonestudios-photos"
EXIF_VERSION = 2  # v2: taken_at auch bei NULL setzen
WORKERS = int(os.environ.get("PHOTON_EXIF_WORKERS", "16"))
HEADER_BYTES = 262144
IMG_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff", ".webp", ".nef", ".cr2"}

ddb = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
s3 = boto3.client("s3", region_name=REGION)

_lock = threading.Lock()
counts = {"checked": 0, "with_camera": 0, "errors": 0}
cameras = Counter()
_t0 = time.time()


def _clean(v):
    if v is None:
        return ""
    if isinstance(v, bytes):
        v = v.decode("utf-8", "ignore")
    return str(v).replace("\x00", "").strip()


def extract(img):
    exif = img.getexif()
    if not exif:
        return {}
    sub = exif.get_ifd(ExifTags.IFD.Exif) or {}
    out = {
        "camera_make": _clean(exif.get(271)),
        "camera_model": _clean(exif.get(272)),
        "lens": _clean(sub.get(42036)),
    }
    dt = _clean(sub.get(36867) or exif.get(306))  # DateTimeOriginal, sonst DateTime
    if len(dt) >= 19 and dt[:4].isdigit() and not dt.startswith("0000"):
        out["taken_at"] = f"{dt[0:4]}-{dt[5:7]}-{dt[8:10]}T{dt[11:19]}"
    return {k: v for k, v in out.items() if v}


def process(item):
    sha, key = item["sha256"], item["s3_key"]
    try:
        obj = s3.get_object(Bucket="photonestudios-photos", Key=key,
                            Range=f"bytes=0-{HEADER_BYTES - 1}")
        data = obj["Body"].read()
        try:
            info = extract(Image.open(io.BytesIO(data)))
        except Exception:
            info = {}
        expr = ["exif_version=:v", "exif_checked_at=:t"]
        vals = {":v": EXIF_VERSION, ":t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        for f in ("camera_make", "camera_model", "lens"):
            if f in info:
                expr.append(f"{f}=:{f}")
                vals[f":{f}"] = info[f]
        if "taken_at" in info and not item.get("taken_at"):
            expr.append("taken_at=:ta")
            vals[":ta"] = info["taken_at"]
        ddb.update_item(Key={"sha256": sha}, UpdateExpression="SET " + ", ".join(expr),
                        ExpressionAttributeValues=vals)
        return info.get("camera_model") and f"{info.get('camera_make', '')} | {info['camera_model']}"
    except Exception as e:
        if os.environ.get("PHOTON_EXIF_DEBUG"):
            print(f"  err {key}: {e!r}", file=sys.stderr)
        return "__error__"


def tally(r):
    with _lock:
        counts["checked"] += 1
        if r == "__error__":
            counts["errors"] += 1
        elif r:
            counts["with_camera"] += 1
            cameras[r] += 1
        c = counts["checked"]
        if c % 500 == 0:
            rate = c / max(1e-9, time.time() - _t0) * 3600
            print(f"  {c} geprueft, {counts['with_camera']} mit Kamera  ({rate:,.0f}/h)", flush=True)


def main():
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    print(f"Kamera-Extraktion startet — {WORKERS} Worker, limit={limit}", flush=True)

    candidates, kw = [], {
        "ProjectionExpression": "sha256, s3_key, taken_at",
        "FilterExpression": "attribute_exists(s3_key) AND (attribute_not_exists(exif_version) OR exif_version < :v)",
        "ExpressionAttributeValues": {":v": EXIF_VERSION},
    }
    while True:
        resp = ddb.scan(**kw)
        for it in resp.get("Items", []):
            if str(it["sha256"]).startswith("__"):
                continue
            if os.path.splitext(it.get("s3_key", ""))[1].lower() not in IMG_EXTS:
                continue
            candidates.append(it)
        if limit and len(candidates) >= limit:
            candidates = candidates[:limit]
            break
        if "LastEvaluatedKey" not in resp:
            break
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    print(f"  {len(candidates)} Kandidaten", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for r in pool.map(process, candidates):
            tally(r)

    dt = time.time() - _t0
    print(f"\nFertig nach {dt:.0f}s — geprueft={counts['checked']} "
          f"mit_Kamera={counts['with_camera']} err={counts['errors']}", flush=True)
    for cam, n in cameras.most_common(30):
        print(f"  {n:6}  {cam}")


if __name__ == "__main__":
    main()
