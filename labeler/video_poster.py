"""PhotonEsTuDios Video-Poster — extrahiert je Video einen Frame (ffmpeg ueber
presigned S3-URL, nur Header+erster GOP statt der ganzen GB-Datei) und legt ihn
als JPEG-Poster nach S3 (thumbs/<sha>_poster.jpg). Die Gallery liefert ihn ueber
/poster/<sha> aus.

Idempotent/resumierbar via attribute_not_exists(poster_version). Lokal, ffmpeg gratis.

  AWS_PROFILE=photon python video_poster.py [--limit N] [--workers N]
"""
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3

REGION = "eu-west-1"
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")
POSTER_VERSION = 1
WORKERS = int(os.environ.get("PHOTON_POSTER_WORKERS", "6"))
VIDEO_EXTS = {".mov", ".mp4", ".m4v"}

ddb = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
s3 = boto3.client("s3", region_name=REGION)

_lock = threading.Lock()
counts = {"checked": 0, "posters": 0, "errors": 0}
_t0 = time.time()


def grab_frame(url, ss):
    cmd = ["ffmpeg", "-y", "-ss", str(ss), "-i", url, "-frames:v", "1",
           "-vf", "scale=480:-1", "-q:v", "4", "-f", "image2pipe",
           "-vcodec", "mjpeg", "pipe:1"]
    p = subprocess.run(cmd, capture_output=True, timeout=45)
    jpg = p.stdout
    return jpg if (p.returncode == 0 and jpg[:2] == b"\xff\xd8") else None


def process(item):
    sha, key = item["sha256"], item["s3_key"]
    try:
        url = s3.generate_presigned_url("get_object",
            Params={"Bucket": BUCKET, "Key": key}, ExpiresIn=900)
        jpg = grab_frame(url, 1) or grab_frame(url, 0)  # 1s, sonst 0s (kurze Clips)
        has = jpg is not None
        if has:
            s3.put_object(Bucket=BUCKET, Key=f"thumbs/{sha}_poster.jpg",
                          Body=jpg, ContentType="image/jpeg")
        ddb.update_item(
            Key={"sha256": sha},
            UpdateExpression="SET poster_version=:v, poster_at=:t, has_poster=:h",
            ExpressionAttributeValues={
                ":v": POSTER_VERSION,
                ":t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                ":h": has,
            },
        )
        return "posters" if has else "checked"
    except Exception as e:
        if os.environ.get("PHOTON_POSTER_DEBUG"):
            print(f"  err {key}: {e!r}", file=sys.stderr)
        return "errors"


def tally(r):
    with _lock:
        counts["checked"] += 1
        if r == "posters":
            counts["posters"] += 1
        elif r == "errors":
            counts["errors"] += 1
        c = counts["checked"]
        if c % 20 == 0:
            rate = c / max(1e-9, time.time() - _t0) * 3600
            print(f"  {c} verarbeitet, {counts['posters']} Poster ({rate:,.0f}/h) err={counts['errors']}", flush=True)


def main():
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    print(f"Video-Poster startet — {WORKERS} Worker, limit={limit}", flush=True)

    cands, kw = [], {
        "ProjectionExpression": "sha256, s3_key",
        "FilterExpression": "attribute_exists(s3_key) AND attribute_not_exists(poster_version)",
    }
    while True:
        resp = ddb.scan(**kw)
        for it in resp.get("Items", []):
            if str(it["sha256"]).startswith("__"):
                continue
            if os.path.splitext(it.get("s3_key", ""))[1].lower() in VIDEO_EXTS:
                cands.append(it)
        if limit and len(cands) >= limit:
            cands = cands[:limit]
            break
        if "LastEvaluatedKey" not in resp:
            break
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    print(f"  {len(cands)} Videos", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for r in pool.map(process, cands):
            tally(r)

    print(f"\nFertig nach {time.time() - _t0:.0f}s — verarbeitet={counts['checked']} "
          f"Poster={counts['posters']} err={counts['errors']}", flush=True)


if __name__ == "__main__":
    main()
