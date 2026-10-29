"""PhotonEsTuDios — S3-Importer (eigene Lambda `photon-s3importer`, eu-west-1).

Importiert einen Rohspiegel s3://photonestudios-photos/raw/<name>/ (z.B. das
Time-Machine-Backup) ins Archiv — komplett in AWS, kein Rechner nötig.
Prinzip wie der Dropbox-Staubsauger: lesen → SHA-256 → Dedup gegen DynamoDB →
Original nach <sha>.<ext> + DynamoDB-Item mit Metadaten.

Zwei Rollen derselben Funktion:
  1. PLAN  (manuell):  {"mode": "plan", "prefix": "raw/timemachine-2024-12-14/",
                        "source": "timemachine:2024-12-14"}
     Liest manifest.tsv + Inhalte der Zips, ÜBERSPRINGT alles, was in
     raw/<name>/_import/results/*.tsv schon als erledigt steht, verteilt den Rest
     in Paketen auf die SQS-Queue und setzt den Fortschrittszähler.
  2. WORKER (SQS-Trigger, parallel per MaximumConcurrency): verarbeitet ein Paket,
     schreibt raw/<name>/_import/results/<messageId>.tsv und zählt in DynamoDB
     (sha256 = "__s3import__:<name>") hoch.

Neu:       Original serverseitig nach <sha>.<ext> kopieren (Zip-Mitglieder: hochladen),
           Item mit source, sources=[source], source_path, raw_key, uploaded_at=mtime, Metadaten.
Duplikat:  nichts kopieren; sources ergänzen, fehlende Metadaten füllen (nie
           überschreiben), Pfad in alt_paths.
Robust:    Zeitbudget je Aufruf; nicht fertige Dateien gehen als neues Paket zurück
           in die Queue (max. 2 Versuche je Datei, dann Fehler). Pakete, die
           den Lambda-Timeout reißen, wiederholt SQS 3x, dann Dead-Letter-Queue.
"""
import concurrent.futures
import datetime as dt
import hashlib
import io
import json
import os
import tempfile
import threading
import time
import urllib.parse
import zipfile
from decimal import Decimal

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

from meta import IMG_EXTS, VID_EXTS, meta_for_file, video_meta

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
QUEUE_URL = os.environ.get("PHOTON_QUEUE_URL", "")
WORKERS = int(os.environ.get("PHOTON_WORKERS", "16"))
BIG = int(os.environ.get("PHOTON_BIG_BYTES", str(200 * 1024 * 1024)))
PKG_FILES = int(os.environ.get("PHOTON_PKG_FILES", "25"))
PKG_BYTES = int(os.environ.get("PHOTON_PKG_BYTES", str(3 * 1024 ** 3)))
SAFETY_MS = int(os.environ.get("PHOTON_SAFETY_MS", "240000"))
CHUNK = 8 * 1024 * 1024
TMPDIR = "/tmp"
MEDIA_EXTS = IMG_EXTS | VID_EXTS | {".avi", ".mts", ".m2ts", ".mkv", ".mpg", ".mpeg", ".wmv", ".gif"}
META_FIELDS = ("taken_at", "camera_make", "camera_model", "lens", "iso", "f_number", "exposure",
               "focal_length", "width", "height", "gps_lat", "gps_lon")

_cfg = Config(connect_timeout=10, read_timeout=60, max_pool_connections=64,
              retries={"max_attempts": 8, "mode": "adaptive"})
s3 = boto3.client("s3", config=_cfg)
sqs = boto3.client("sqs", config=_cfg)
table = boto3.resource("dynamodb", config=_cfg).Table(TABLE)


def _now():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0).isoformat() + "Z"


def _iso_utc(epoch):
    return dt.datetime.fromtimestamp(float(epoch), dt.timezone.utc).replace(tzinfo=None, microsecond=0).isoformat() + "Z"


def _progress_key(name):
    return {"sha256": f"__s3import__:{name}"}


# ---- S3 als (seekbare) Datei ------------------------------------------------
def _range(key, off, n, size):
    if off >= size:
        return b""
    return s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes={off}-{min(off + n, size) - 1}")["Body"].read()


class _S3File(io.RawIOBase):
    def __init__(self, key, size):
        self.key, self.size, self.pos = key, size, 0
    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.pos
    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else self.pos + off if whence == 1 else self.size + off
        return self.pos
    def readinto(self, b):
        data = _range(self.key, self.pos, len(b), self.size)
        b[:len(data)] = data
        self.pos += len(data)
        return len(data)


_zips = {}
_zlock = threading.Lock()


def _zip(key, size):
    with _zlock:
        if key not in _zips:
            _zips.clear()
            _zips[key] = zipfile.ZipFile(io.BufferedReader(_S3File(key, size), buffer_size=4 * 1024 * 1024))
        return _zips[key]


# ---- Kern ------------------------------------------------------------------
def _hash_to_tmp(stream, suffix):
    h = hashlib.sha256(); n = 0
    fd, path = tempfile.mkstemp(dir=TMPDIR, suffix=suffix)
    with os.fdopen(fd, "wb") as f:
        while True:
            c = stream.read(CHUNK)
            if not c:
                break
            h.update(c); f.write(c); n += len(c)
    return path, h.hexdigest(), n


def _hash_only(stream):
    h = hashlib.sha256(); n = 0
    while True:
        c = stream.read(CHUNK)
        if not c:
            break
        h.update(c); n += len(c)
    return h.hexdigest(), n


def _mime(ext):
    return {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".heic": "image/heic",
            ".heif": "image/heif", ".gif": "image/gif", ".tif": "image/tiff", ".tiff": "image/tiff",
            ".mov": "video/quicktime", ".mp4": "video/mp4", ".m4v": "video/x-m4v",
            ".avi": "video/x-msvideo"}.get(ext, "application/octet-stream")


def _s3meta(u, source):
    name = os.path.basename(u.get("member") or u["path"])
    return {"original-filename": urllib.parse.quote(name)[:1000], "source": source}


def _copy(u, dest, ext, source, size):
    extra = {"ContentType": _mime(ext), "Metadata": _s3meta(u, source), "MetadataDirective": "REPLACE"}
    if size < 5 * 1024 ** 3:
        s3.copy_object(Bucket=BUCKET, Key=dest, CopySource={"Bucket": BUCKET, "Key": u["key"]}, **extra)
    else:  # > 5 GB: verwaltete Multipart-Kopie (serverseitig)
        s3.copy({"Bucket": BUCKET, "Key": u["key"]}, BUCKET, dest,
                ExtraArgs={"ContentType": extra["ContentType"], "Metadata": extra["Metadata"],
                           "MetadataDirective": "REPLACE"},
                Config=TransferConfig(multipart_chunksize=256 * 1024 * 1024, max_concurrency=8))


def _register(sha, ext, size, u, source, meta, put_original):
    existing = table.get_item(Key={"sha256": sha}).get("Item")
    if existing:
        changed = False
        srcs = list(existing.get("sources") or ([existing["source"]] if existing.get("source") else []))
        if source not in srcs:
            srcs.append(source); existing["sources"] = srcs; changed = True
        for k in META_FIELDS:
            if meta.get(k) is not None and existing.get(k) in (None, ""):
                existing[k] = meta[k]; changed = True
        if existing.get("taken_at") and not existing.get("ym"):
            existing["ym"] = existing["taken_at"][:7]; changed = True
        alt = list(existing.get("alt_paths") or [])
        if u["path"] not in alt and existing.get("source_path") != u["path"] and len(alt) < 20:
            alt.append(u["path"]); existing["alt_paths"] = alt; changed = True
        if changed:
            existing["enriched_at"] = _now()
            table.put_item(Item=existing)
        return "dup"
    dest = f"{sha}{ext}"
    put_original(dest)
    item = {
        "sha256": sha, "s3_bucket": BUCKET, "s3_key": dest,
        "original_filename": os.path.basename(u.get("member") or u["path"]),
        "source_path": u["path"], "source": source, "sources": [source],
        "raw_key": u["key"], "bytes": Decimal(size), "mime": _mime(ext),
        "uploaded_at": _iso_utc(u.get("mtime") or time.time()), "imported_at": _now(),
    }
    item.update({k: v for k, v in meta.items() if v is not None})
    if item.get("taken_at"):
        item["ym"] = item["taken_at"][:7]
    table.put_item(Item=item)
    return "new"


def process(u, source):
    name = u.get("member") or u["path"]
    ext = os.path.splitext(name)[1].lower()
    if ext not in MEDIA_EXTS:
        return {"key": u["key"], "status": "skip", "size": u["size"]}
    size = int(u["size"]); tmp = None
    try:
        if u.get("member"):
            z = _zip(u["zip_key"], int(u["zip_size"]))
            with z.open(u["member"]) as src:
                tmp, sha, size = _hash_to_tmp(src, ext)
            meta = meta_for_file(tmp, ext, size)
            st = _register(sha, ext, size, u, source, meta,
                           lambda d: s3.upload_file(tmp, BUCKET, d, ExtraArgs={"ContentType": _mime(ext), "Metadata": _s3meta(u, source)}))
        elif size <= BIG:
            body = s3.get_object(Bucket=BUCKET, Key=u["key"])["Body"]
            tmp, sha, size = _hash_to_tmp(body, ext)
            meta = meta_for_file(tmp, ext, size)
            st = _register(sha, ext, size, u, source, meta, lambda d: _copy(u, d, ext, source, size))
        else:
            sha, size = _hash_only(s3.get_object(Bucket=BUCKET, Key=u["key"])["Body"])
            meta = video_meta(lambda off, n: _range(u["key"], off, n, size), size) if ext in VID_EXTS else {}
            st = _register(sha, ext, size, u, source, meta, lambda d: _copy(u, d, ext, source, size))
        return {"key": u["key"], "sha": sha, "status": st, "size": size}
    except Exception as e:
        print(f"s3import-error {u['key']}: {e!r}")
        return {"key": u["key"], "status": "err", "error": repr(e)[:500], "size": u["size"]}
    finally:
        if tmp and os.path.exists(tmp):
            os.remove(tmp)


# ---- PLAN --------------------------------------------------------------------
def _done_keys(prefix):
    done = set()
    pg = s3.get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=BUCKET, Prefix=prefix + "_import/results/"):
        for o in page.get("Contents", []):
            body = s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read().decode("utf-8", "replace")
            for line in body.splitlines():
                p = line.split("\t")
                if len(p) >= 3 and p[2] in ("new", "dup", "skip"):
                    done.add(p[0])
    return done


def plan(event, context):
    prefix = event["prefix"].rstrip("/") + "/"
    source = event["source"]
    name = prefix.rstrip("/").split("/")[-1]
    rows = [l.split("\t") for l in s3.get_object(Bucket=BUCKET, Key=prefix + "manifest.tsv")["Body"].read()
            .decode("utf-8").splitlines()[1:] if l.strip()]
    rel = lambda src: src.split("/Users/potstupimi/", 1)[-1]
    units = []
    for key, size, mtime, src in rows:
        if not key.lower().endswith(".zip"):
            units.append({"key": key, "size": int(size), "mtime": int(mtime), "path": rel(src)})
        else:
            z = zipfile.ZipFile(io.BufferedReader(_S3File(key, int(size)), buffer_size=1024 * 1024))
            for m in z.infolist():
                ext = os.path.splitext(m.filename)[1].lower()
                if m.is_dir() or os.path.basename(m.filename).startswith(".") or ext not in MEDIA_EXTS:
                    continue
                units.append({"key": f"{key}!{m.filename}", "size": m.file_size,
                              "mtime": int(dt.datetime(*m.date_time).timestamp()),
                              "path": f"{rel(src)}!{m.filename}", "member": m.filename,
                              "zip_key": key, "zip_size": int(size)})
    done = _done_keys(prefix)
    todo = [u for u in units if u["key"] not in done]
    # Pakete: Zip-Mitglieder nach Zip gruppiert (Zip-Verzeichnis nur einmal lesen)
    todo.sort(key=lambda u: (u.get("zip_key", ""), u["key"]))
    pkgs, cur, cb = [], [], 0
    for u in todo:
        if cur and (len(cur) >= PKG_FILES or cb + u["size"] > PKG_BYTES or u.get("zip_key") != cur[-1].get("zip_key")):
            pkgs.append(cur); cur, cb = [], 0
        cur.append(u); cb += u["size"]
    if cur:
        pkgs.append(cur)
    table.put_item(Item={**_progress_key(name), "source": source, "prefix": prefix, "state": "läuft",
                         "planned_at": _now(), "updated_at": _now(),
                         "total_files": len(units), "total_bytes": sum(u["size"] for u in units),
                         "already_files": len(units) - len(todo),
                         "already_bytes": sum(u["size"] for u in units if u["key"] in done),
                         "planned_files": len(todo), "planned_bytes": sum(u["size"] for u in todo),
                         "packages": len(pkgs), "packages_done": 0,
                         "done_files": 0, "done_bytes": 0, "new": 0, "dup": 0, "skip": 0, "err": 0})
    entries = [{"Id": str(i), "MessageBody": json.dumps({"name": name, "prefix": prefix, "source": source, "units": p})}
               for i, p in enumerate(pkgs)]
    for i in range(0, len(entries), 10):
        r = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries[i:i + 10])
        if r.get("Failed"):
            raise RuntimeError(f"SQS: {r['Failed'][:3]}")
    msg = f"Plan {name}: {len(units)} gesamt, {len(units) - len(todo)} schon erledigt, {len(todo)} in {len(pkgs)} Paketen"
    print(msg)
    return {"message": msg}


# ---- WORKER ----------------------------------------------------------------
def _work_package(msg_id, body, context):
    name, prefix, source, units = body["name"], body["prefix"], body["source"], body["units"]
    res_key = f"{prefix}_import/results/{msg_id}.tsv"
    try:  # SQS-Wiederholung eines schon fertigen Pakets -> nichts doppelt zählen
        s3.head_object(Bucket=BUCKET, Key=res_key)
        return
    except ClientError:
        pass
    left = lambda: context.get_remaining_time_in_millis() if context else 10 ** 9
    results, unfinished = [], []
    pool = concurrent.futures.ThreadPoolExecutor(WORKERS)
    futs, idx = {}, 0
    try:
        while idx < len(units) or futs:
            while idx < len(units) and len(futs) < WORKERS and left() > SAFETY_MS:
                futs[pool.submit(process, units[idx], source)] = units[idx]; idx += 1
            if idx < len(units) and left() <= SAFETY_MS:
                unfinished += units[idx:]; idx = len(units)
            if not futs:
                break
            if left() < 60000:  # Notbremse: Hänger zurück in die Queue
                unfinished += list(futs.values()); break
            done, _ = concurrent.futures.wait(list(futs), timeout=5, return_when=concurrent.futures.FIRST_COMPLETED)
            for f in done:
                futs.pop(f); results.append(f.result())
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    retry = []
    for u in unfinished:
        u["attempt"] = u.get("attempt", 0) + 1
        if u["attempt"] >= 3:
            results.append({"key": u["key"], "status": "err", "error": "Zeitlimit (3 Versuche)", "size": u["size"]})
        else:
            retry.append(u)
    if retry:
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps({**body, "units": retry}))

    lines = "".join(f"{r['key']}\t{r.get('sha', '')}\t{r['status']}\t{(r.get('error') or '').replace(chr(9), ' ').replace(chr(10), ' ')}\n"
                    for r in results)
    s3.put_object(Bucket=BUCKET, Key=res_key, Body=lines.encode())
    c = {k: sum(1 for r in results if r["status"] == k) for k in ("new", "dup", "skip", "err")}
    ok_bytes = sum(int(r.get("size") or 0) for r in results if r["status"] != "err")
    table.update_item(
        Key=_progress_key(name),
        UpdateExpression="ADD #n :n, dup :d, skip :s, err :e, done_files :f, done_bytes :b, packages_done :p SET updated_at = :u",
        ExpressionAttributeNames={"#n": "new"},
        ExpressionAttributeValues={":n": c["new"], ":d": c["dup"], ":s": c["skip"], ":e": c["err"],
                                   ":f": len(results) - c["err"], ":b": ok_bytes, ":p": 0 if retry else 1, ":u": _now()})
    for r in results:
        if r["status"] == "err":
            table.put_item(Item={"sha256": f"__s3err__:{name}:{r['key']}"[:1024], "error": r.get("error", ""), "at": _now()})
    print(f"Paket {msg_id}: {c}, zurück in Queue {len(retry)}, {len(results)} fertig")


def handler(event, context):
    if isinstance(event, dict) and "Records" in event:
        failures = []
        for rec in event["Records"]:
            try:
                _work_package(rec["messageId"], json.loads(rec["body"]), context)
            except Exception as e:
                print(f"Paket {rec['messageId']} fehlgeschlagen: {e!r}")
                failures.append({"itemIdentifier": rec["messageId"]})
        return {"batchItemFailures": failures}
    if event.get("mode") == "plan":
        return plan(event, context)
    return {"error": "unbekannter Aufruf", "event": str(event)[:200]}
