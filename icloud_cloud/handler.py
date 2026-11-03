"""PhotonEsTuDios — iCloud-Fotos ins Archiv (staubsauger#2). Komplett in AWS.

Zwei Lambdas aus demselben Zip:

  photon-icloud-plan   (handler.plan_handler, reserved concurrency 1, darf das Secret lesen)
      Öffnet die gespeicherte iCloud-Sitzung, listet „Alle Fotos“ seitenweise und
      vergleicht jedes Asset OHNE Download mit dem Archiv (matching.py):
        - schon importiert (icloud_id bekannt)        → nichts
        - Treffer im Archiv (z. B. Dropbox-Kopie)     → Paket "enrich": GPS/Zeit/Favorit nachtragen
        - kein Treffer                                → Paket "import": Original laden
      Pakete gehen über SQS an den Worker. Läuft die Zeit ab, ruft sich der Plan
      selbst mit dem Offset wieder auf. {"dry_run": true} zählt nur.
      Zeitplan: täglich (EventBridge).

  photon-icloud-worker (handler.worker_handler, SQS-Trigger, parallel, KEIN Secret-Zugriff)
      Lädt über die vom Plan erzeugten Download-Links (kein Apple-Login nötig),
      SHA-256 → Dedup → S3 + DynamoDB wie Dropbox-Ingest, source="icloud".

Idempotent: S3-Key/PK = SHA-256, Metadaten nur ergänzen (if_not_exists), nie überschreiben.
"""
import datetime as dt
import hashlib
import json
import os
import shutil
import time
import urllib.parse
import urllib.request
from decimal import Decimal

import boto3
from botocore.config import Config

import session_store
from matching import ArchiveIndex

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-archive")
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
QUEUE_URL = os.environ.get("PHOTON_QUEUE_URL", "")
BUDGET_S = int(os.environ.get("PHOTON_PLAN_BUDGET_S", "600"))
PAGE = 100
PKG_IMPORT = int(os.environ.get("PHOTON_PKG_IMPORT", "5"))
PKG_ENRICH = int(os.environ.get("PHOTON_PKG_ENRICH", "50"))
PROGRESS = {"sha256": "__icloud__"}
MATCH_KEYS = {"name+bytes": "match_name", "zeit+maße": "match_time_dims", "zeit": "match_time"}
SESSION_DIR = "/tmp/icloud-session"

_cfg = Config(connect_timeout=10, read_timeout=60, max_pool_connections=16,
              retries={"max_attempts": 8, "mode": "adaptive"})
_clients = {}


def _c(name):
    if name not in _clients:
        _clients[name] = boto3.client(name, config=_cfg)
    return _clients[name]


def _table():
    if "table" not in _clients:
        _clients["table"] = boto3.resource("dynamodb", config=_cfg).Table(TABLE)
    return _clients["table"]


def _now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---- Fortschritt (für status.py + Gallery) ------------------------------------
def _progress(**fields):
    names = {f"#{k}": k for k in fields}
    vals = {f":{k}": v for k, v in fields.items()}
    _table().update_item(Key=PROGRESS,
                         UpdateExpression="SET " + ", ".join(f"#{k}=:{k}" for k in fields),
                         ExpressionAttributeNames=names, ExpressionAttributeValues=vals)


def _add_counts(counts):
    if not counts:
        return
    names = {f"#{k}": k for k in counts}
    vals = {f":{k}": Decimal(v) for k, v in counts.items()}
    _table().update_item(Key=PROGRESS,
                         UpdateExpression="ADD " + ", ".join(f"#{k} :{k}" for k in counts),
                         ExpressionAttributeNames=names, ExpressionAttributeValues=vals)


# ---- Plan ----------------------------------------------------------------------
def build_index(table=None):
    table = table or _table()
    idx = ArchiveIndex()
    kw = {"ProjectionExpression": "sha256, s3_key, original_filename, #b, taken_at, width, height, gps_lat, icloud_id",
          "ExpressionAttributeNames": {"#b": "bytes"}}
    while True:
        r = table.scan(**kw)
        for it in r["Items"]:
            idx.add(it)
        if "LastEvaluatedKey" not in r:
            return idx
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def classify(metas, idx):
    """Liste von asset_meta -> (enrich, imports, counts). Rein, testbar."""
    enrich, imports = [], []
    c = {"seen": 0, "known": 0, "matched": 0, "new": 0, "gps_gain": 0}
    for m in metas:
        c["seen"] += 1
        if m["icloud_id"] in idx.icloud_ids:
            c["known"] += 1
            continue
        sha, how = idx.match(m)
        if sha:
            c["matched"] += 1
            k = MATCH_KEYS[how]
            c[k] = c.get(k, 0) + 1
            if m.get("gps") and sha not in idx.has_gps:
                c["gps_gain"] += 1
            enrich.append({**m, "sha256": sha, "match": how})
        else:
            c["new"] += 1
            imports.append(m)
    return enrich, imports, c


def _chunks(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


def _send(op, items, size):
    entries = [{"Id": str(i), "MessageBody": json.dumps({"op": op, "items": pkg}, default=str)}
               for i, pkg in enumerate(_chunks(items, size))]
    for batch in _chunks(entries, 10):
        r = _c("sqs").send_message_batch(QueueUrl=QUEUE_URL, Entries=batch)
        if r.get("Failed"):
            raise RuntimeError(f"SQS failed: {r['Failed']}")


def cap_page(page, offset, limit):
    """LIMIT exakt einhalten (Apple liefert ganze 100er-Seiten)."""
    return page[:max(limit - offset, 0)] if limit else page


def plan_handler(event, context):
    from assets import asset_meta
    from pyicloud.services.photos_cloudkit import DirectionEnum

    event = event or {}
    t0 = time.time()
    offset = int(event.get("offset") or 0)
    limit = int(event.get("limit") or 0)
    dry = bool(event.get("dry_run"))
    run_id = event.get("run_id") or _now()
    sm = _c("secretsmanager")

    shutil.rmtree(SESSION_DIR, ignore_errors=True)
    try:
        apple_id = session_store.load(sm, SESSION_DIR)
        svc = session_store.open_service(apple_id, SESSION_DIR)
    except session_store.LoginNeeded as e:
        _progress(status="login_needed", status_at=_now(), error=str(e))
        print(f"login-needed: {e}")
        return {"status": "login_needed"}

    if offset == 0:
        _table().put_item(Item={**PROGRESS, "run_id": run_id, "status": "listing", "dry_run": dry,
                                "started_at": _now(), "status_at": _now(), "offset": 0})
    album = svc.photos.all
    total = len(album)
    _progress(total=total, status_at=_now())
    idx = build_index()
    print(f"plan run={run_id} offset={offset} total={total} dry={dry} index={len(idx.by_time)}t")

    done = False
    while True:
        page = cap_page(list(album._get_photos_at(offset, DirectionEnum.ASCENDING, PAGE)), offset, limit)
        if not page:
            done = True
            break
        metas = [asset_meta(a) for a in page]
        enrich, imports, counts = classify(metas, idx)
        if not dry:
            if enrich:
                _send("enrich", enrich, PKG_ENRICH)
            if imports:
                _send("import", imports, PKG_IMPORT)
        _add_counts(counts)
        offset += len(page)
        _progress(offset=offset, status_at=_now())
        if offset >= total or (limit and offset >= limit):
            done = True
            break
        if time.time() - t0 > BUDGET_S:
            break

    # Sitzung zurückschreiben: Apple frischt Tokens auf, so hält sie länger.
    session_store.save(sm, apple_id, SESSION_DIR, _now())

    if done:
        _progress(status="dry_run_done" if dry else "queued", status_at=_now(), finished_at=_now())
        return {"status": "done", "offset": offset, "total": total}
    _c("lambda").invoke(FunctionName=context.function_name, InvocationType="Event",
                        Payload=json.dumps({**event, "offset": offset, "run_id": run_id}).encode())
    return {"status": "continued", "offset": offset, "total": total}


# ---- Worker ----------------------------------------------------------------------
def _gps_update(m):
    if not m.get("gps"):
        return {}
    return {"gps_lat": Decimal(m["gps"][0]), "gps_lon": Decimal(m["gps"][1])}


def enrich_one(m, table=None):
    """Vorhandenen Archiv-Eintrag um iCloud-Wissen ergänzen. Überschreibt nie."""
    table = table or _table()
    sets = {"icloud_id": m["icloud_id"], "icloud_match": m.get("match") or "sha256",
            "icloud_favorite": m.get("favorite", False), "icloud_at": _now()}
    keep = {"taken_at": m.get("taken_at"), "taken_at_utc": m.get("taken_at_utc"),
            "tz_offset": m.get("tz_offset"), **_gps_update(m)}
    if m.get("taken_at"):
        keep["ym"] = m["taken_at"][:7]
    keep = {k: v for k, v in keep.items() if v is not None}
    names = {f"#{k}": k for k in {**sets, **keep}}
    vals = {f":{k}": v for k, v in {**sets, **keep}.items()}
    expr = [f"#{k}=:{k}" for k in sets] + [f"#{k}=if_not_exists(#{k}, :{k})" for k in keep]
    if "gps_lat" in keep:
        vals[":gps_src"] = "icloud"
        names["#gps_src"] = "gps_source"
        expr.append("#gps_src=if_not_exists(#gps_src, :gps_src)")
    table.update_item(Key={"sha256": m["sha256"]}, UpdateExpression="SET " + ", ".join(expr),
                      ExpressionAttributeNames=names, ExpressionAttributeValues=vals,
                      ConditionExpression="attribute_exists(sha256)")


def _download(url, path):
    h = hashlib.sha256()
    n = 0
    req = urllib.request.Request(url, headers={"User-Agent": "photon-icloud/1"})
    with urllib.request.urlopen(req, timeout=60) as r, open(path, "wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
            f.write(chunk)
            n += len(chunk)
    return h.hexdigest(), n


def import_one(m):
    """-> 'uploaded' | 'duplicate' ."""
    from meta import meta_for_file

    if not m.get("url"):
        raise RuntimeError("kein Download-Link")
    path = f"/tmp/icloud-{m['icloud_id'].replace('/', '_')}{m['ext']}"
    try:
        sha, size = _download(m["url"], path)
        table = _table()
        if table.get_item(Key={"sha256": sha}, ProjectionExpression="sha256").get("Item"):
            enrich_one({**m, "sha256": sha, "match": "sha256"}, table)
            return "duplicate"
        key = f"{sha}{m['ext']}"
        _c("s3").upload_file(path, BUCKET, key, ExtraArgs={"Metadata": {
            "original-filename": urllib.parse.quote(m["filename"]), "source": "icloud"}})
        try:
            exif = {k: v for k, v in (meta_for_file(path, m["ext"], size) or {}).items() if v is not None}
        except Exception as e:  # Metadaten dürfen den Import nie kippen
            print(f"meta-error {m['icloud_id']}: {e!r}")
            exif = {}
        item = {**exif,  # EXIF zuerst, iCloud-Werte nur wo EXIF nichts hat
                "sha256": sha, "s3_bucket": BUCKET, "s3_key": key, "bytes": size,
                "original_filename": m["filename"], "source": "icloud",
                "source_path": f"icloud:{m['icloud_id']}", "uploaded_at": _now(),
                "icloud_id": m["icloud_id"], "icloud_favorite": m.get("favorite", False),
                "icloud_at": _now()}
        for k in ("taken_at", "taken_at_utc", "tz_offset", "width", "height"):
            if item.get(k) is None and m.get(k) is not None:
                item[k] = m[k]
        if item.get("gps_lat") is None and m.get("gps"):
            item.update(_gps_update(m), gps_source="icloud")
        if item.get("taken_at"):
            item["ym"] = str(item["taken_at"])[:7]
        table.put_item(Item=item)
        return "uploaded"
    finally:
        if os.path.exists(path):
            os.remove(path)


def worker_handler(event, context):
    failures = []
    for rec in event.get("Records", []):
        body = json.loads(rec["body"])
        counts = {}
        try:
            for m in body["items"]:
                if body["op"] == "enrich":
                    try:
                        enrich_one(m)
                        r = "enriched"
                    except _table().meta.client.exceptions.ConditionalCheckFailedException:
                        r = "enrich_missing"
                else:
                    r = import_one(m)
                counts[r] = counts.get(r, 0) + 1
        except Exception as e:
            print(f"worker-error op={body.get('op')}: {e!r}")
            failures.append({"itemIdentifier": rec["messageId"]})
        finally:
            _add_counts(counts)
    return {"batchItemFailures": failures}
