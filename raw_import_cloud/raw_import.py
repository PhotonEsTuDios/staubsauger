"""PhotonEsTuDios — Raw-Import (Lambda-Modus "raw-import").

Importiert Dateien aus einem S3-Rohspiegel (raw/<name>/…) ins Archiv:
  stream → SHA-256 → Dedup (DynamoDB) → <sha>.<ext> in S3 + DynamoDB-Item.
Bei Duplikaten: Herkunft in `sources` ergänzen + fehlende Metadaten nachtragen.

Läuft in der bestehenden Lambda photon-dropbox-ingest (handler.py verzweigt bei
event.mode == "raw-import"). Deren Rolle hat KEIN s3:GetObject → gelesen wird über
presigned GET-URLs, die der Treiber (driver.py) mit Danielas User erzeugt.
In-Region S3→Lambda kostet keinen Transfer.

Event:
  {"mode": "raw-import", "source": "timemachine:2024-12-14",
   "items": [{"key": raw-key, "url": presigned, "size": n, "mtime": epoch,
              "path": relativer Originalpfad,
              # optional Zip-Mitglied:
              "member": "name/im/zip", "zip_url": presigned-URL des Zips}]}
Antwort:
  {"results": [{"key", "sha", "status": new|dup|skip|err, "error"?}], "pending": [keys…]}
Zeitbudget: neue Items werden nur gestartet, solange > SAFETY_MS Restzeit.
"""
import concurrent.futures
import datetime as dt
import hashlib
import io
import os
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from decimal import Decimal

import boto3
from boto3.s3.transfer import TransferConfig

from meta import IMG_EXTS, VID_EXTS, meta_for_file, video_meta

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
WORKERS = int(os.environ.get("PHOTON_RAW_WORKERS", "24"))
BIG = int(os.environ.get("PHOTON_RAW_BIG_BYTES", str(200 * 1024 * 1024)))
SAFETY_MS = int(os.environ.get("PHOTON_RAW_SAFETY_MS", "300000"))
CHUNK = 8 * 1024 * 1024
TMPDIR = "/tmp"
MEDIA_EXTS = IMG_EXTS | VID_EXTS | {".avi", ".mts", ".m2ts", ".mkv", ".mpg", ".mpeg", ".wmv", ".gif"}

_s3 = boto3.client("s3")
_table = boto3.resource("dynamodb").Table(TABLE)
_big_sem = threading.Semaphore(2)  # große Dateien: höchstens 2 gleichzeitig


# ---- HTTP (presigned) ------------------------------------------------------
def _get(url, rng=None, timeout=30, tries=5):
    """GET mit kurzem Socket-Timeout + Wiederholung. Ein hängender TLS-Handshake
    (kam vor) blockierte vorher bis zu 300 s und bremste den ganzen Batch."""
    req = urllib.request.Request(url, headers={"Range": f"bytes={rng[0]}-{rng[1]}"} if rng else {})
    for attempt in range(tries):
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError:
            raise
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(1 + attempt * 2)


def _range_reader(url, size):
    def read(off, n):
        if off >= size:
            return b""
        with _get(url, (off, min(off + n, size) - 1)) as r:
            return r.read()
    return read


class _HttpFile(io.RawIOBase):
    """Seekbare Datei über HTTP-Range (für zipfile auf riesigen Zips)."""
    def __init__(self, url, size):
        self.url, self.size, self.pos = url, size, 0
    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.pos
    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else self.pos + off if whence == 1 else self.size + off
        return self.pos
    def readinto(self, b):
        if self.pos >= self.size:
            return 0
        n = min(len(b), self.size - self.pos)
        with _get(self.url, (self.pos, self.pos + n - 1)) as r:
            data = r.read()
        b[:len(data)] = data
        self.pos += len(data)
        return len(data)


_zip_cache = {}
_zip_lock = threading.Lock()


def _zip(url, size, key):
    with _zip_lock:
        z = _zip_cache.get(key)
        if z is None:
            z = zipfile.ZipFile(io.BufferedReader(_HttpFile(url, size), buffer_size=4 * 1024 * 1024))
            _zip_cache.clear(); _zip_cache[key] = z
        return z


# ---- Kern ------------------------------------------------------------------
def _stream_to_tmp(stream, suffix):
    h = hashlib.sha256(); n = 0
    fd, path = tempfile.mkstemp(dir=TMPDIR, suffix=suffix)
    with os.fdopen(fd, "wb") as f:
        while True:
            c = stream.read(CHUNK)
            if not c:
                break
            h.update(c); f.write(c); n += len(c)
    return path, h.hexdigest(), n


def _hash_stream(stream):
    h = hashlib.sha256(); n = 0
    while True:
        c = stream.read(CHUNK)
        if not c:
            break
        h.update(c); n += len(c)
    return h.hexdigest(), n


def _iso_utc(epoch):
    return dt.datetime.fromtimestamp(float(epoch), dt.timezone.utc).replace(tzinfo=None, microsecond=0).isoformat() + "Z"


def _mime(ext):
    return {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".heic": "image/heic",
            ".heif": "image/heif", ".gif": "image/gif", ".tif": "image/tiff", ".tiff": "image/tiff",
            ".mov": "video/quicktime", ".mp4": "video/mp4", ".m4v": "video/x-m4v",
            ".avi": "video/x-msvideo"}.get(ext, "application/octet-stream")


META_FIELDS = ("taken_at", "camera_make", "camera_model", "lens", "iso", "f_number", "exposure",
               "focal_length", "width", "height", "gps_lat", "gps_lon")


def _register(sha, ext, size, it, source, meta, upload):
    """Dedup + DynamoDB. upload() lädt das Original nach <sha><ext> (nur bei neu)."""
    existing = _table.get_item(Key={"sha256": sha}).get("Item")
    if existing:
        changed = False
        srcs = list(existing.get("sources") or ([existing["source"]] if existing.get("source") else []))
        if source not in srcs:
            srcs.append(source); existing["sources"] = srcs; changed = True
        for k in META_FIELDS:  # nur Lücken füllen, nie überschreiben
            if meta.get(k) is not None and existing.get(k) in (None, ""):
                existing[k] = meta[k]; changed = True
        if existing.get("taken_at") and not existing.get("ym"):
            existing["ym"] = existing["taken_at"][:7]; changed = True
        alt = list(existing.get("alt_paths") or [])
        if it["path"] not in alt and existing.get("source_path") != it["path"] and len(alt) < 20:
            alt.append(it["path"]); existing["alt_paths"] = alt; changed = True
        if changed:
            existing["enriched_at"] = _iso_utc(time.time())
            _table.put_item(Item=existing)
        return "dup"
    key = f"{sha}{ext}"
    upload(key)
    item = {
        "sha256": sha, "s3_bucket": BUCKET, "s3_key": key,
        "original_filename": os.path.basename(it.get("member") or it["path"]),
        "source_path": it["path"], "source": source, "sources": [source],
        "raw_key": it["key"], "bytes": Decimal(size), "mime": _mime(ext),
        "uploaded_at": _iso_utc(it.get("mtime") or time.time()),
        "imported_at": _iso_utc(time.time()),
    }
    item.update({k: v for k, v in meta.items() if v is not None})
    if item.get("taken_at"):
        item["ym"] = item["taken_at"][:7]
    _table.put_item(Item=item)
    return "new"


def _s3_meta(it, source):
    name = os.path.basename(it.get("member") or it["path"])
    return {"original-filename": urllib.parse.quote(name)[:1000], "source": source}


def _process(it, source):
    key = it["key"]
    name = it.get("member") or it["path"]
    ext = os.path.splitext(name)[1].lower()
    if ext not in MEDIA_EXTS:
        return {"key": key, "status": "skip"}
    size = int(it["size"])
    tmp = None
    try:
        if it.get("member"):  # Zip-Mitglied → /tmp
            z = _zip(it["zip_url"], int(it["zip_size"]), it["zip_key"])
            with z.open(it["member"]) as src:
                tmp, sha, size = _stream_to_tmp(src, ext)
            meta = meta_for_file(tmp, ext, size)
            st = _register(sha, ext, size, it, source, meta,
                           lambda k: _s3.upload_file(tmp, BUCKET, k, ExtraArgs={"ContentType": _mime(ext), "Metadata": _s3_meta(it, source)}))
        elif size <= BIG:
            with _get(it["url"], timeout=60) as r:
                tmp, sha, size = _stream_to_tmp(r, ext)
            meta = meta_for_file(tmp, ext, size)
            st = _register(sha, ext, size, it, source, meta,
                           lambda k: _s3.upload_file(tmp, BUCKET, k, ExtraArgs={"ContentType": _mime(ext), "Metadata": _s3_meta(it, source)}))
        else:  # groß: 1. Durchlauf hashen, 2. Durchlauf streamend hochladen
            with _big_sem:
                with _get(it["url"], timeout=60) as r:
                    sha, size = _hash_stream(r)
                meta = video_meta(_range_reader(it["url"], size), size) if ext in VID_EXTS else {}
                def up(k):
                    with _get(it["url"], timeout=60) as r2:
                        _s3.upload_fileobj(r2, BUCKET, k, ExtraArgs={"ContentType": _mime(ext), "Metadata": _s3_meta(it, source)},
                                           Config=TransferConfig(multipart_chunksize=64 * 1024 * 1024, max_concurrency=4))
                st = _register(sha, ext, size, it, source, meta, up)
        return {"key": key, "sha": sha, "status": st}
    except Exception as e:
        print(f"raw-import-error {key}: {e!r}")
        return {"key": key, "status": "err", "error": repr(e)[:500]}
    finally:
        if tmp and os.path.exists(tmp):
            os.remove(tmp)


def handle(event, context):
    source = event["source"]
    items = event.get("items", [])
    results, pending, hung = [], [], []
    t0 = time.time()

    def time_left_ms():
        return context.get_remaining_time_in_millis() if context else 10 ** 9

    # Kein "with": bei hängenden Threads (z.B. defekte Datei) nicht auf sie warten,
    # sondern kurz vor dem Lambda-Timeout mit allen fertigen Ergebnissen zurückkehren.
    pool = concurrent.futures.ThreadPoolExecutor(WORKERS)
    futs = {}
    idx = 0
    try:
        while idx < len(items) or futs:
            while idx < len(items) and len(futs) < WORKERS and time_left_ms() > SAFETY_MS:
                futs[pool.submit(_process, items[idx], source)] = items[idx]; idx += 1
            if idx < len(items) and time_left_ms() <= SAFETY_MS:
                pending += [x["key"] for x in items[idx:]]; idx = len(items)
            if not futs:
                break
            if time_left_ms() < 45000:  # Notbremse: Rest als hängend melden
                hung = [x["key"] for x in futs.values()]
                break
            done, _ = concurrent.futures.wait(list(futs), timeout=5, return_when=concurrent.futures.FIRST_COMPLETED)
            for f in done:
                futs.pop(f)
                results.append(f.result())
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    print(f"raw-import batch: {len(results)} fertig, {len(pending)} offen, {len(hung)} hängend, {time.time()-t0:.0f}s")
    return {"results": results, "pending": pending, "hung": hung}


def zip_list(event, context):
    """Mitglieder eines Zips auflisten (für den Treiber)."""
    z = zipfile.ZipFile(io.BufferedReader(_HttpFile(event["url"], int(event["size"])), buffer_size=1024 * 1024))
    out = []
    for m in z.infolist():
        if m.is_dir() or os.path.basename(m.filename).startswith("."):
            continue
        ext = os.path.splitext(m.filename)[1].lower()
        if ext in MEDIA_EXTS:
            out.append({"member": m.filename, "size": m.file_size,
                        "mtime": dt.datetime(*m.date_time).timestamp()})
    return {"members": out}
