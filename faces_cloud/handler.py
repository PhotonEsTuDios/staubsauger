"""PhotonEsTuDios — Gesichtserkennung in der Cloud (Lambda `photon-faces`, staubsauger#14).

Nur Ablauf; Bildlogik in detect.py, Gruppieren in cluster.py, Personen in people.py, Speicher in store.py.

  {"mode": "plan", "limit": N}  Fotos ohne face_version in Paketen auf die SQS-Queue (täglich).
  SQS-Records                    Worker: Gesichter finden, Vektor + Ausschnitt speichern.
  {"mode": "cluster"}            Alle Gesichter zu Personen-Gruppen bündeln (täglich nach dem Plan).
  {"mode": "name"|"hide"|"reject"|"name_face", ...}  Benennen aus der Gallery (people.py).
  {"mode": "sample", "cluster", "n"?, "offset"?}       Stichprobe zum Prüfen (gallery#10).
  {"mode": "confirm", "cluster", "person", "wrong", "shown"}  Name + falsche raus (gallery#10).
"""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.exceptions import ClientError

import cluster as clu
import detect
import people
from store import Store

QUEUE_URL = os.environ.get("PHOTON_QUEUE_URL", "")
WORKERS = int(os.environ.get("PHOTON_WORKERS", "3"))
PKG = int(os.environ.get("PHOTON_PKG", "25"))
PEOPLE_MODES = {"name": ("cluster", "person"), "hide": ("cluster",),
                "reject": ("sha", "face"), "name_face": ("sha", "face", "person"),
                "confirm": ("cluster", "person", "wrong", "shown")}
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

_store = None
_pool = ThreadPoolExecutor(max_workers=WORKERS)  # bleibt warm -> Modelle je Thread nur einmal laden


def store():
    global _store
    if _store is None:
        _store = Store()
    return _store


# ---- Plan ----------------------------------------------------------------------
def plan(event, sqs=None):
    sqs = sqs or boto3.client("sqs")
    todo = store().photos_todo(PHOTO_EXTS, int(event.get("limit") or 0))
    pkgs = [todo[i:i + PKG] for i in range(0, len(todo), PKG)]
    for i in range(0, len(pkgs), 10):
        entries = [{"Id": str(j), "MessageBody": json.dumps(p)} for j, p in enumerate(pkgs[i:i + 10])]
        res = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
        if res.get("Failed"):
            raise RuntimeError(f"SQS: {res['Failed'][:2]}")
    store().start_progress(total=len(todo), packages=len(pkgs))
    out = {"todo": len(todo), "packages": len(pkgs)}
    print("[plan]", out)
    return out


# ---- Worker ----------------------------------------------------------------------
def process(task):
    """Ein Foto -> ('ok', n_gesichter) | ('skip', 0) | ('err', 0)."""
    st = store()
    try:
        if st.already_done(task["sha"]):
            return "skip", 0
        faces = detect.find_faces(detect.load_image(st.read_original(task["key"])))
        st.save_faces(task["sha"], faces, detect.vec_to_bytes)
        return "ok", len(faces)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return "skip", 0
        err = e.response["Error"]["Code"]
    except Exception as e:  # kaputte Datei darf das Paket nicht kippen
        err = f"{type(e).__name__}: {str(e)[:200]}"
    print(f"[err] {task['key']}: {err}")
    try:
        st.mark_error(task["sha"], err)
    except Exception:
        pass
    return "err", 0


def work(tasks):
    t0 = time.time()
    results = list(_pool.map(process, tasks))
    done = sum(1 for r, _ in results if r == "ok")
    faces = sum(n for _, n in results)
    err = sum(1 for r, _ in results if r == "err")
    store().add_progress(done, faces, err)
    print(f"[paket] {len(tasks)} in {time.time() - t0:.1f}s ok={done} faces={faces} err={err}")


def handler(event, context):
    mode = event.get("mode")
    if mode == "plan":
        return plan(event)
    if mode == "cluster":
        return people.run_cluster(store(), float(event.get("threshold") or clu.THRESHOLD))
    if mode == "sample":
        return people.sample(store(), event["cluster"], int(event.get("n") or people.SAMPLE_N),
                             int(event.get("offset") or 0))
    if mode in PEOPLE_MODES:  # aus der Gallery (gallery#7/#9), synchron
        args = {k: event[k] for k in PEOPLE_MODES[mode]}
        return getattr(people, mode)(store(), **args)
    failures = []
    for rec in event.get("Records", []):
        try:
            work(json.loads(rec["body"]))
        except Exception as e:
            print(f"[paket-fehler] {e!r}")
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}
