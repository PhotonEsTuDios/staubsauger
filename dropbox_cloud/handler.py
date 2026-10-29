"""
PhotonEsTuDios — Dropbox-Cloud-Ingest (AWS Lambda, eu-west-1).

Zieht Fotos & Videos DIREKT von Dropbox nach S3 + DynamoDB — läuft in der
AWS-Cloud, NICHT über Danielas Rechner. Zeitgesteuert via EventBridge.

Secrets (Dropbox App-Key/-Secret + Refresh-Token) liegen in AWS Secrets Manager
unter `photonestudios/dropbox` — NICHT im Repo.

Dedup identisch zur lokalen Pipeline: SHA-256 als S3-Key & DynamoDB-PK.
Inkrementell über einen Dropbox-Cursor (DynamoDB sha256="__dropbox_cursor__").

SPEICHER-SICHER (seit 2026-06-19, 2. Iteration):
  Frühere Version lud jede Datei komplett in den RAM (blob = r.read()). Bei 12
  Workern und Videos bis >1 GB sprengte das die Lambda → OOM → Prozess gekillt
  → Cursor blieb stehen → Quasi-Stillstand. Jetzt wird JEDE Datei chunked nach
  /tmp gestreamt (SHA wird beim Schreiben mitgerechnet) und von Platte zu S3
  hochgeladen. RAM bleibt konstant, unabhängig von der Dateigröße. /tmp
  (EphemeralStorage) muss > größte Datei sein.

BEOBACHTBARKEIT ohne CloudWatch-Recht:
  Jeder Lauf schreibt einen Run-Record (sha256="__dropbox_run__") und pro Fehler
  ein Item (sha256="__dxerr__:<pfad>") in DynamoDB. status.py liest beides →
  Daniela sieht ohne logs:-Recht, was passiert ist und was schiefging.
"""
import concurrent.futures
import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
SECRET_NAME = os.environ.get("PHOTON_DROPBOX_SECRET", "photonestudios/dropbox")
DROPBOX_FOLDER = os.environ.get("PHOTON_DROPBOX_FOLDER", "")  # "" = gesamtes App-/Root-Verzeichnis
CURSOR_PK = "__dropbox_cursor__"
RUN_PK = "__dropbox_run__"
ERR_PREFIX = "__dxerr__:"

# Worker für kleine Dateien (I/O-gebunden). Große Dateien laufen mit weniger
# Parallelität, damit das geteilte /tmp nicht überläuft (Summe gleichzeitiger
# Downloads auf Platte muss < EphemeralStorage bleiben).
MAX_WORKERS = int(os.environ.get("PHOTON_INGEST_WORKERS", "12"))
LARGE_WORKERS = int(os.environ.get("PHOTON_LARGE_WORKERS", "2"))
LARGE_THRESHOLD = int(os.environ.get("PHOTON_LARGE_BYTES", str(20 * 1024 * 1024)))
CHUNK = 1024 * 1024  # 1 MB Streaming-Chunks
# Invocation sauber beenden, solange noch so viel Zeit übrig ist (ms). Verhindert
# Tod mitten in einer Seite → Cursor steht immer auf sauberer Seitengrenze.
SAFETY_MS = int(os.environ.get("PHOTON_SAFETY_MS", "120000"))
TMPDIR = "/tmp"

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


def _download_to_file(token, path, tries=4):
    """Streamt eine Datei chunked nach /tmp und berechnet dabei den SHA-256.
    Liefert (tmp_pfad, sha_hex, bytes). RAM-Verbrauch = ein Chunk, NICHT die
    ganze Datei. Wiederholt bei 429 / transienten Netzfehlern mit Backoff;
    räumt bei Fehlschlag die halbe Temp-Datei weg."""
    req = urllib.request.Request(
        "https://content.dropboxapi.com/2/files/download",
        headers={"Authorization": f"Bearer {token}",
                 "Dropbox-API-Arg": json.dumps({"path": path})},
    )
    for attempt in range(tries):
        tmp_path = None
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                fd, tmp_path = tempfile.mkstemp(dir=TMPDIR)
                h = hashlib.sha256()
                size = 0
                with os.fdopen(fd, "wb") as f:
                    while True:
                        chunk = r.read(CHUNK)
                        if not chunk:
                            break
                        h.update(chunk)
                        f.write(chunk)
                        size += len(chunk)
                return tmp_path, h.hexdigest(), size
        except urllib.error.HTTPError as exc:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            if exc.code == 429 and attempt < tries - 1:
                retry_after = exc.headers.get("Retry-After")
                time.sleep(min(int(retry_after) if retry_after else 2, 30))
                continue
            raise
        except urllib.error.URLError:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            if attempt < tries - 1:
                time.sleep(2 * (attempt + 1))
                continue
            raise


def _load_cursor():
    item = _table.get_item(Key={"sha256": CURSOR_PK}).get("Item")
    return item.get("cursor") if item else None


def _save_cursor(cursor):
    _table.put_item(Item={"sha256": CURSOR_PK, "cursor": cursor})


def _record_error(entry, exc, size=None):
    """Fehlgeschlagene Datei in DynamoDB festhalten (idempotent über Pfad) —
    für gezieltes Retry und Sichtbarkeit ohne CloudWatch."""
    path = entry.get("path_display") or entry.get("path_lower") or entry.get("name", "?")
    try:
        _table.put_item(Item={
            "sha256": ERR_PREFIX + path[:1900],
            "source_path": path,
            "original_filename": entry.get("name"),
            "bytes": size if size is not None else entry.get("size"),
            "error": repr(exc)[:800],
            "failed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
    except Exception as e:  # Logging darf den Ingest nie kippen
        print(f"record-error-failed {path}: {e!r}")


def _process_entry(token, entry):
    """Eine Datei streamen → hashen → dedup → S3+DynamoDB. Idempotent: S3-Key
    und DynamoDB-PK SIND der SHA-256 des Inhalts. Liefert
    'uploaded' | 'skipped' | 'error'."""
    name = entry["name"]
    ext = os.path.splitext(name)[1].lower()
    tmp_path = None
    size = None
    try:
        tmp_path, sha, size = _download_to_file(token, entry["path_lower"])
        if _table.get_item(Key={"sha256": sha}).get("Item"):
            return "skipped"

        key = f"{sha}{ext}"
        # S3-Metadaten MÜSSEN ASCII sein — deutsche Dateinamen (ä/ö/ü) sprengen
        # sonst den Upload. Daher URL-kodiert ablegen; der echte Name steht
        # ungekürzt in DynamoDB (original_filename).
        _s3.upload_file(tmp_path, BUCKET, key,
                        ExtraArgs={"Metadata": {
                            "original-filename": urllib.parse.quote(name),
                            "source": "dropbox"}})
        _table.put_item(Item={
            "sha256": sha,
            "s3_bucket": BUCKET,
            "s3_key": key,
            "original_filename": name,
            "source_path": entry["path_display"],
            "source": "dropbox",
            "bytes": size,
            "taken_at": (entry.get("media_info", {})
                         .get("metadata", {}).get("time_taken")),
            "dropbox_rev": entry.get("rev"),
            "uploaded_at": entry.get("server_modified"),
        })
        return "uploaded"
    except Exception as exc:  # eine defekte Datei darf nicht die Seite kippen
        print(f"ingest-error {entry.get('path_display')}: {exc!r}")
        _record_error(entry, exc, size)
        return "error"
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


def _clean_tmp():
    """Reste eines zuvor gekillten Laufs entfernen (Lambda-Container werden
    wiederverwendet → /tmp könnte sonst volllaufen)."""
    try:
        for n in os.listdir(TMPDIR):
            p = os.path.join(TMPDIR, n)
            try:
                if os.path.isfile(p):
                    os.remove(p)
            except OSError:
                pass
    except OSError:
        pass


def _process_page(token, page, counts):
    """Verarbeitet die Datei-Einträge einer Seite: kleine Dateien hoch-parallel,
    große mit reduzierter Parallelität (schont das geteilte /tmp)."""
    files = [e for e in page.get("entries", [])
             if e.get(".tag") == "file"
             and os.path.splitext(e["name"])[1].lower() in IMAGE_EXTS]
    small = [e for e in files if (e.get("size") or 0) < LARGE_THRESHOLD]
    large = [e for e in files if (e.get("size") or 0) >= LARGE_THRESHOLD]

    last_path = None
    for batch, workers in ((small, MAX_WORKERS), (large, LARGE_WORKERS)):
        if not batch:
            continue
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            for entry, result in zip(batch, pool.map(
                    lambda e: _process_entry(token, e), batch)):
                counts[result] = counts.get(result, 0) + 1
                last_path = entry.get("path_display") or last_path
    return last_path


def _save_run(counts, pages, last_path, reason, started):
    try:
        _table.put_item(Item={
            "sha256": RUN_PK,
            "uploaded": counts.get("uploaded", 0),
            "skipped": counts.get("skipped", 0),
            "errors": counts.get("error", 0),
            "pages": pages,
            "last_path": last_path,
            "stopped_reason": reason,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
            "ended_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "duration_s": int(time.time() - started),
        })
    except Exception as e:
        print(f"save-run-failed: {e!r}")


def handler(event, context):
    started = time.time()
    _clean_tmp()
    sec = _secret()
    token = _access_token(sec)
    cursor = _load_cursor()

    counts = {}
    pages = 0
    last_path = None
    reason = "complete"
    first = True
    prev_cursor = cursor

    while True:
        # Zeitbudget: lieber sauber beenden als mitten in einer Seite gekillt
        # werden. Cursor steht durch das Per-Seite-Speichern stets sauber.
        if context is not None and context.get_remaining_time_in_millis() < SAFETY_MS:
            reason = "time-budget"
            break

        if first:
            page = (_rpc(token, "files/list_folder/continue", {"cursor": prev_cursor})
                    if prev_cursor else
                    _rpc(token, "files/list_folder",
                         {"path": DROPBOX_FOLDER, "recursive": True}))
            first = False
        else:
            page = _rpc(token, "files/list_folder/continue", {"cursor": prev_cursor})

        lp = _process_page(token, page, counts)
        last_path = lp or last_path
        pages += 1

        # Cursor erst nach komplett verarbeiteter Seite — Timeout/Tod mittendrin
        # = Seite wird neu geladen, Dedup fängt die schon geladenen ab.
        prev_cursor = page["cursor"]
        _save_cursor(prev_cursor)

        if not page.get("has_more"):
            break

    _save_run(counts, pages, last_path, reason, started)
    return {"uploaded": counts.get("uploaded", 0),
            "skipped": counts.get("skipped", 0),
            "errors": counts.get("error", 0),
            "pages": pages, "stopped_reason": reason}


if __name__ == "__main__":  # lokaler Smoke-Test (braucht AWS-Profil photon)
    print(handler({}, None))
