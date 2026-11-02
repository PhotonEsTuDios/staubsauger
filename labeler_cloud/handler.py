"""PhotonEsTuDios — Auto-Labeler in der Cloud (Lambda `photon-labeler`, staubsauger#6).

Labelt das ganze Archiv komplett in AWS, der Mac stößt nur an. Muster wie s3importer:

  1. PLAN  (täglich per EventBridge oder manuell): {"mode": "plan"}
     Scannt DynamoDB nach Medien ohne Label (Fotos: label_version fehlt,
     Videos: poster_version fehlt), verteilt sie in Paketen auf die SQS-Queue
     und setzt den Fortschrittszähler (sha256 = "__labeler__").
     Neue Fotos aus Dropbox/Import werden so spätestens am nächsten Tag gelabelt.
  2. WORKER (SQS-Trigger, parallel): ein Paket = Liste von {sha, key, kind}.
     Foto:  Qualität/Schärfe/Farbe (labeler/quality.py, dieselbe Formel wie lokal)
            + fehlende EXIF-Felder (Kamera, Aufnahmezeit, GPS) nachtragen, nie überschreiben.
     Video: Poster-Frame per ffmpeg (Layer) über lokalen S3-Range-Proxy -> thumbs/<sha>_poster.jpg.

Idempotent: jedes Update mit attribute_not_exists(label_version/poster_version),
doppelte Pakete oder Wiederholungen schaden nicht. Kaputte Dateien werden mit
label_error markiert und beim nächsten Plan nicht erneut versucht.
"""
import datetime as dt
import http.server
import io
import json
import os
import re
import subprocess
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from PIL import Image, ImageFile

from meta import image_meta
from quality import LABEL_VERSION, MAXDIM, analyze

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-archive")
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
QUEUE_URL = os.environ.get("PHOTON_QUEUE_URL", "")
WORKERS = int(os.environ.get("PHOTON_WORKERS", "4"))
PKG = int(os.environ.get("PHOTON_PKG", "40"))
FFMPEG = os.environ.get("PHOTON_FFMPEG", "/opt/bin/ffmpeg")
POSTER_VERSION = 1

PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"}
VIDEO_EXTS = {".mov", ".mp4", ".m4v"}
EXIF_FIELDS = ("taken_at", "camera_make", "camera_model", "lens", "iso", "f_number",
               "exposure", "focal_length", "width", "height", "gps_lat", "gps_lon")
PROGRESS = {"sha256": "__labeler__"}

Image.MAX_IMAGE_PIXELS = 300_000_000  # Panoramen ok, Dekompressionsbomben nicht
ImageFile.LOAD_TRUNCATED_IMAGES = True

_cfg = Config(connect_timeout=10, read_timeout=60, max_pool_connections=32,
              retries={"max_attempts": 8, "mode": "adaptive"})
s3 = boto3.client("s3", config=_cfg)
sqs = boto3.client("sqs", config=_cfg)
table = boto3.resource("dynamodb", config=_cfg).Table(TABLE)


def _now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ext(key):
    return os.path.splitext(key)[1].lower()


# ---- Plan --------------------------------------------------------------------
def plan(event):
    limit = int(event.get("limit") or 0)
    kw = {
        "ProjectionExpression": "sha256, s3_key, label_version, poster_version, label_error",
        "FilterExpression": "attribute_exists(s3_key) AND attribute_not_exists(label_error) AND "
                            "(attribute_not_exists(label_version) OR attribute_not_exists(poster_version))",
    }
    todo = []
    while True:
        r = table.scan(**kw)
        for it in r.get("Items", []):
            sha, key = str(it["sha256"]), it.get("s3_key", "")
            if sha.startswith("__"):
                continue
            ext = _ext(key)
            if ext in PHOTO_EXTS and "label_version" not in it:
                todo.append({"sha": sha, "key": key, "kind": "photo"})
            elif ext in VIDEO_EXTS and "poster_version" not in it:
                todo.append({"sha": sha, "key": key, "kind": "video"})
        if "LastEvaluatedKey" not in r or (limit and len(todo) >= limit):
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    if limit:
        todo = todo[:limit]

    pkgs = [todo[i:i + PKG] for i in range(0, len(todo), PKG)]
    for i in range(0, len(pkgs), 10):
        entries = [{"Id": str(j), "MessageBody": json.dumps(p)} for j, p in enumerate(pkgs[i:i + 10])]
        res = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
        if res.get("Failed"):
            raise RuntimeError(f"SQS: {res['Failed'][:2]}")

    n_photo = sum(1 for t in todo if t["kind"] == "photo")
    table.put_item(Item={**PROGRESS, "planned_at": _now(), "updated_at": _now(),
                         "total": len(todo), "photos": n_photo, "videos": len(todo) - n_photo,
                         "packages": len(pkgs), "packages_done": 0,
                         "labeled": 0, "posters": 0, "exif_filled": 0, "err": 0})
    out = {"todo": len(todo), "photos": n_photo, "videos": len(todo) - n_photo, "packages": len(pkgs)}
    print("[plan]", out)
    return out


# ---- Worker: Foto --------------------------------------------------------------
def label_photo(sha, key):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    img = Image.open(io.BytesIO(body))
    img.draft("RGB", (MAXDIM, MAXDIM))  # JPEG direkt verkleinert dekodieren: schneller, wenig RAM
    m = analyze(img)

    names = {"#lv": "label_version"}
    values = {":s": m["sharpness"], ":b": m["brightness"], ":c": m["contrast"],
              ":q": m["quality_score"], ":bl": m["blurry"], ":dc": m["dominant_colors"],
              ":t": _now(), ":v": LABEL_VERSION, ":d": ["quality", "color"]}
    sets = ["sharpness=:s", "brightness=:b", "contrast=:c", "quality_score=:q", "blurry=:bl",
            "dominant_colors=:dc", "labeled_at=:t", "#lv=:v", "label_dims=:d"]

    # fehlende EXIF-Felder nachtragen (if_not_exists = nie überschreiben)
    filled = 0
    try:
        meta = image_meta(io.BytesIO(body))
    except Exception:
        meta = {}
    for i, f in enumerate(EXIF_FIELDS):
        if f in meta:
            names[f"#e{i}"] = f
            values[f":e{i}"] = meta[f]
            sets.append(f"#e{i}=if_not_exists(#e{i}, :e{i})")
            filled = 1
    if "gps_lat" in meta:
        names["#gs"] = "gps_source"
        values[":gs"] = "exif"
        sets.append("#gs=if_not_exists(#gs, :gs)")

    table.update_item(Key={"sha256": sha}, UpdateExpression="SET " + ", ".join(sets),
                      ConditionExpression="attribute_not_exists(#lv)",
                      ExpressionAttributeNames=names, ExpressionAttributeValues=values)
    return "labeled", filled


# ---- Worker: Video-Poster ------------------------------------------------------
# Das statische ffmpeg kann in Lambda keine Hostnamen auflösen ("Input/output error"
# bei der presigned S3-URL). Deshalb ein Mini-Proxy auf 127.0.0.1: ffmpeg fragt
# http://127.0.0.1:<port>/<key> mit Range-Headern, der Proxy holt genau diese
# Bytes per boto3 aus S3. So wird nie das ganze (oft GB-große) Video geladen.
class _S3Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, head_only):
        key = urllib.parse.unquote(self.path.lstrip("/"))
        try:
            size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        except ClientError:
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
        m = re.match(r"bytes=(\d+)-(\d*)", self.headers.get("Range", ""))
        start = int(m.group(1)) if m else 0
        end = int(m.group(2)) if m and m.group(2) else size - 1
        if start >= size:
            self.send_response(416); self.send_header("Content-Range", f"bytes */{size}")
            self.send_header("Content-Length", "0"); self.end_headers(); return
        end = min(end, size - 1)
        self.send_response(206 if m else 200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(end - start + 1))
        if m:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if head_only:
            return
        body = s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes={start}-{end}")["Body"]
        try:
            for chunk in body.iter_chunks(256 * 1024):
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass  # ffmpeg springt woanders hin und schließt die Verbindung
        finally:
            body.close()

    def do_GET(self):
        self._send(False)

    def do_HEAD(self):
        self._send(True)


_proxy_port = None
_proxy_lock = threading.Lock()


def _proxy_url(key):
    global _proxy_port
    with _proxy_lock:
        if _proxy_port is None:
            srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _S3Proxy)
            srv.daemon_threads = True
            threading.Thread(target=srv.serve_forever, daemon=True).start()
            _proxy_port = srv.server_address[1]
    return f"http://127.0.0.1:{_proxy_port}/{urllib.parse.quote(key)}"


def _grab(url, ss):
    cmd = [FFMPEG, "-nostdin", "-loglevel", "error", "-y", "-ss", str(ss), "-i", url,
           "-frames:v", "1", "-vf", "scale=480:-2", "-q:v", "4",
           "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
    p = subprocess.run(cmd, capture_output=True, timeout=90)
    if p.returncode == 0 and p.stdout[:2] == b"\xff\xd8":
        return p.stdout, ""
    return None, (p.stderr.decode("utf8", "replace").strip().splitlines() or [f"rc={p.returncode}"])[-1][:200]


def poster_video(sha, key):
    url = _proxy_url(key)
    jpg, why = _grab(url, 1)  # 1 s rein (kein Schwarzbild)
    if not jpg:
        jpg, why = _grab(url, 0)  # sehr kurze Videos: erstes Bild
    if not jpg:
        raise RuntimeError(f"ffmpeg: {why}")
    s3.put_object(Bucket=BUCKET, Key=f"thumbs/{sha}_poster.jpg", Body=jpg,
                  ContentType="image/jpeg", CacheControl="public, max-age=31536000, immutable")
    table.update_item(Key={"sha256": sha},
                      UpdateExpression="SET poster_version=:v, poster_at=:t",
                      ConditionExpression="attribute_not_exists(poster_version)",
                      ExpressionAttributeValues={":v": POSTER_VERSION, ":t": _now()})
    return "posters", 0


def _one(task):
    try:
        if task["kind"] == "photo":
            return label_photo(task["sha"], task["key"])
        return poster_video(task["sha"], task["key"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return "skip", 0  # schon erledigt (doppeltes Paket)
        err = f"{e.response['Error']['Code']}"
    except Exception as e:  # kaputte Datei darf das Paket nicht kippen
        err = f"{type(e).__name__}: {str(e)[:200]}"
    print(f"[err] {task['key']}: {err}")
    try:
        table.update_item(Key={"sha256": task["sha"]}, UpdateExpression="SET label_error=:e, label_error_at=:t",
                          ExpressionAttributeValues={":e": err, ":t": _now()})
    except Exception:
        pass
    return "err", 0


def work(tasks):
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(_one, tasks))
    c = {"labeled": 0, "posters": 0, "err": 0, "skip": 0}
    filled = 0
    for r, f in results:
        c[r] += 1
        filled += f
    table.update_item(
        Key=PROGRESS,
        UpdateExpression="ADD labeled :l, posters :p, err :e, exif_filled :x, packages_done :one SET updated_at=:t",
        ExpressionAttributeValues={":l": c["labeled"], ":p": c["posters"], ":e": c["err"],
                                   ":x": filled, ":one": 1, ":t": _now()})
    print(f"[paket] {len(tasks)} in {time.time() - t0:.1f}s {c} exif+{filled}")


def handler(event, context):
    if event.get("mode") == "plan":
        return plan(event)
    failures = []
    for rec in event.get("Records", []):
        try:
            work(json.loads(rec["body"]))
        except Exception as e:
            print(f"[paket-fehler] {e!r}")
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}
