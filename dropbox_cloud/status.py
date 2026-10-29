"""
PhotonEsTuDios — Dropbox-Ingest Status.

Zeigt auf einen Blick:
  - geladene Bilder (DynamoDB, source=dropbox) vs. Dropbox-Gesamt + Prozent + Rest
  - Ø-Rate und ETA *seit dem letzten Aufruf* (Checkpoint in DynamoDB)
  - Lambda-Zustand (Memory, Worker, reserved concurrency) und Zeitplan

  AWS_PROFILE=photon ~/PhotonEsTuDios/.venv/bin/python status.py [--full]

Standard: Gesamtzahl wird aus EXPECTED_TOTAL geschaetzt (env PHOTON_DROPBOX_TOTAL),
damit der Status waehrend des Backfills KEIN Dropbox-API-Budget frisst.
Mit --full wird Dropbox komplett neu gelistet (exakt, aber langsamer + API-Last).

Jeder Aufruf schreibt einen Checkpoint (sha256="__ingest_status__") mit count+ts,
um beim naechsten Mal die Rate zu berechnen. Diese Zeile zaehlt NICHT als Foto
(kein source-Feld), stoert den Index also nicht.
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from decimal import Decimal

import boto3

EXPECTED_TOTAL = int(os.environ.get("PHOTON_DROPBOX_TOTAL", "55184"))
STATUS_PK = "__ingest_status__"
RUN_PK = "__dropbox_run__"
ERR_PREFIX = "__dxerr__:"
FUNCTION = os.environ.get("PHOTON_FUNCTION", "photon-dropbox-ingest")
RULE = os.environ.get("PHOTON_RULE", "photon-dropbox-hourly")
REGION = "eu-west-1"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff",
              ".nef", ".cr2", ".cr3", ".arw", ".dng", ".raf",
              ".mov", ".mp4", ".m4v"}

sm = boto3.client("secretsmanager", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION).Table(os.environ.get("PHOTON_TABLE", "photonestudios-photos"))
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def access_token():
    sec = json.loads(sm.get_secret_value(SecretId="photonestudios/dropbox")["SecretString"])
    data = urllib.parse.urlencode({
        "grant_type": "refresh_token", "refresh_token": sec["refresh_token"],
        "client_id": sec["app_key"], "client_secret": sec["app_secret"]}).encode()
    return json.load(urllib.request.urlopen(
        urllib.request.Request("https://api.dropboxapi.com/oauth2/token", data=data), timeout=30))["access_token"]


def rpc(token, endpoint, body):
    req = urllib.request.Request(f"https://api.dropboxapi.com/2/{endpoint}",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=60))


def dropbox_total():
    token = access_token()
    total = 0
    page = rpc(token, "files/list_folder", {"path": "", "recursive": True})
    while True:
        for e in page.get("entries", []):
            if e.get(".tag") == "file" and os.path.splitext(e["name"])[1].lower() in IMAGE_EXTS:
                total += 1
        if not page.get("has_more"):
            break
        page = rpc(token, "files/list_folder/continue", {"cursor": page["cursor"]})
    return total


def done_count():
    n = 0
    kw = {"Select": "COUNT", "FilterExpression": "#s = :d",
          "ExpressionAttributeNames": {"#s": "source"},
          "ExpressionAttributeValues": {":d": "dropbox"}}
    while True:
        r = ddb.scan(**kw)
        n += r["Count"]
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return n


def load_checkpoint():
    item = ddb.get_item(Key={"sha256": STATUS_PK}).get("Item")
    if item and "ts" in item and "count" in item:
        return float(item["ts"]), int(item["count"])
    return None, None


def save_checkpoint(ts, count):
    ddb.put_item(Item={"sha256": STATUS_PK, "ts": Decimal(str(round(ts, 3))), "count": count})


def fmt_eta(hours):
    if hours is None:
        return "n/a"
    if hours < 1:
        return f"~{hours * 60:.0f} min"
    return f"~{hours:.1f} h"


def fmt_dur(seconds):
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"


def lambda_state():
    out = {}
    try:
        cfg = lam.get_function_configuration(FunctionName=FUNCTION)
        out["mem"] = cfg.get("MemorySize")
        out["timeout"] = cfg.get("Timeout")
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        out["workers"] = env.get("PHOTON_INGEST_WORKERS", "default(12)")
    except Exception as exc:
        out["cfg_error"] = repr(exc)
    try:
        fn = lam.get_function(FunctionName=FUNCTION)
        out["concurrency"] = (fn.get("Concurrency") or {}).get("ReservedConcurrentExecutions", "unset")
    except Exception:
        out["concurrency"] = "n/a (kein GetFunction-Recht)"
    return out


def schedule_state():
    try:
        r = events.describe_rule(Name=RULE)
        return f"{r.get('ScheduleExpression')} [{r.get('State')}]"
    except Exception as exc:
        return f"n/a ({exc!r})"


def last_run():
    """Run-Record des letzten Lambda-Laufs (von handler.py geschrieben)."""
    return ddb.get_item(Key={"sha256": RUN_PK}).get("Item")


def error_items(limit=20):
    """Fehler-Items (__dxerr__:<pfad>) zählen + Beispiele sammeln."""
    n = 0
    samples = []
    kw = {"FilterExpression": "begins_with(sha256, :p)",
          "ExpressionAttributeValues": {":p": ERR_PREFIX},
          "ProjectionExpression": "source_path, #e, #b",
          "ExpressionAttributeNames": {"#e": "error", "#b": "bytes"}}
    while True:
        r = ddb.scan(**kw)
        n += r["Count"]
        for it in r["Items"]:
            if len(samples) < limit:
                samples.append(it)
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return n, samples


def main():
    full = "--full" in sys.argv
    now = time.time()
    prev_ts, prev_count = load_checkpoint()
    done = done_count()
    total = dropbox_total() if full else EXPECTED_TOTAL
    total_label = "exakt" if full else "≈ erwartet"
    pct = (done / total * 100) if total else 0
    remaining = max(total - done, 0)

    rate_h = None
    elapsed = (now - prev_ts) if prev_ts else None
    new_imgs = (done - prev_count) if prev_count is not None else None
    if elapsed and elapsed > 0 and new_imgs is not None and new_imgs > 0:
        rate_h = new_imgs / elapsed * 3600
    eta_h = (remaining / rate_h) if rate_h else None
    save_checkpoint(now, done)

    print(f"Dropbox-Ingest: {done}/{total} Bilder ({total_label})  —  {pct:.1f}%  —  Rest {remaining}")
    if prev_ts is None or new_imgs is None:
        print("Rate: (erster Aufruf — beim naechsten Check gibt's Rate+ETA)")
    else:
        seit = time.strftime("%Y-%m-%d %H:%M", time.localtime(prev_ts))
        fenster = f"Fenster {fmt_dur(elapsed)}, seit {seit}"
        if new_imgs == 0:
            print(f"Rate: 0/h — KEINE neuen Bilder ({fenster}) ⚠️ Ingest steht")
        else:
            warn = "  ⚠️ kurzes Fenster, Rate verrauscht" if elapsed < 600 else ""
            print(f"Rate (seit letztem Check): ~{rate_h:,.0f}/h   ETA: {fmt_eta(eta_h)}"
                  f"   [+{new_imgs} in {fmt_dur(elapsed)}]{warn}")
    ls = lambda_state()
    print(f"Lambda: {ls.get('mem')} MB, {ls.get('workers')} Worker, "
          f"reserved concurrency = {ls.get('concurrency')}")
    print(f"Zeitplan: {schedule_state()}")

    run = last_run()
    if run:
        print(f"Letzter Lauf: {run.get('ended_at')}  "
              f"+{run.get('uploaded', 0)} hochgeladen, "
              f"{run.get('skipped', 0)} übersprungen, "
              f"{run.get('errors', 0)} Fehler  "
              f"({run.get('pages', 0)} Seiten, {run.get('duration_s', '?')}s, "
              f"Ende: {run.get('stopped_reason', '?')})")
        if run.get("last_path"):
            print(f"  zuletzt bei: {run.get('last_path')}")
    else:
        print("Letzter Lauf: (noch kein Run-Record — alte Lambda-Version?)")

    if "--errors" in sys.argv:
        n_err, samples = error_items()
        print(f"Fehlerliste (__dxerr__): {n_err} Eintraege")
        for it in samples:
            b = it.get("bytes")
            mb = f"{int(b)/1024/1024:.0f}MB " if b else ""
            print(f"  - {mb}{it.get('source_path')}  →  {it.get('error', '')[:140]}")


if __name__ == "__main__":
    main()
