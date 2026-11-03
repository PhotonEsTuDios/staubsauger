"""PhotonEsTuDios — Gesichtserkennung in der Cloud (Lambda `photon-faces`, staubsauger#14).

Nur Ablauf; Bildlogik in detect.py, Gruppieren in cluster.py, Speicher in store.py.

  {"mode": "plan", "limit": N}  Fotos ohne face_version in Paketen auf die SQS-Queue (täglich).
  SQS-Records                    Worker: Gesichter finden, Vektor + Ausschnitt speichern.
  {"mode": "cluster"}            Alle Gesichter zu Personen-Gruppen bündeln (täglich nach dem Plan).
"""
import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.exceptions import ClientError

import cluster as clu
import detect
from store import Store

QUEUE_URL = os.environ.get("PHOTON_QUEUE_URL", "")
WORKERS = int(os.environ.get("PHOTON_WORKERS", "3"))
PKG = int(os.environ.get("PHOTON_PKG", "25"))
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

_store = None
_pool = ThreadPoolExecutor(max_workers=WORKERS)  # bleibt warm -> Modelle je Thread nur einmal laden
WRITERS = int(os.environ.get("PHOTON_WRITERS", "16"))


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


# ---- Clustering ---------------------------------------------------------------------
def run_cluster(event=None):
    st = store()
    t0 = time.time()
    items = st.all_faces()
    if not items:
        return {"faces": 0, "clusters": 0}
    vecs = clu.np.stack([detect.bytes_to_vec(it["vec"]) for it in items])
    labels = clu.cluster(vecs, float((event or {}).get("threshold") or clu.THRESHOLD))
    named = clu.persons_for_clusters(labels, [it.get("person") for it in items])

    writes = []
    old_photo, new_photo, face_persons = defaultdict(set), defaultdict(set), defaultdict(set)
    for it, c in zip(items, labels):
        sha = str(it["sha256"])
        if it.get("cluster") != c:
            writes.append((st.set_cluster, (sha, int(it["face"]), c)))
        if it.get("cluster"):
            old_photo[sha].add(it["cluster"])
        if c:
            new_photo[sha].add(c)
        if it.get("person"):
            face_persons[sha].add(it["person"])

    changed = len(writes)
    photos_changed = 0
    for sha in set(old_photo) | set(new_photo) | set(face_persons):
        new = new_photo.get(sha, set())
        persons = {named[c] for c in new if c in named} | face_persons.get(sha, set())
        if new != old_photo.get(sha, set()) or persons:
            writes.append((st.set_photo_people, (sha, sorted(new), sorted(persons))))
            photos_changed += 1
    with ThreadPoolExecutor(max_workers=WRITERS) as pool:
        list(pool.map(lambda w: w[0](*w[1]), writes))

    groups = defaultdict(list)
    for it, c in zip(items, labels):
        if c:
            groups[c].append(it)
    summary = {"generated_at": st.now(), "faces": len(items), "clusters": []}
    for c in sorted(groups):
        g = groups[c]
        best = sorted(g, key=lambda it: -float(it["score"]) * int(it["px"]))[:12]
        summary["clusters"].append({
            "cluster": c, "size": len(g), "photos": len({str(it["sha256"]) for it in g}),
            "person": named.get(c), "samples": [it["crop"] for it in best]})
    st.write_summary(summary)
    out = {"faces": len(items), "clusters": len(groups), "faces_changed": changed,
           "photos_changed": photos_changed, "seconds": round(time.time() - t0, 1)}
    print("[cluster]", out)
    return out


def handler(event, context):
    mode = event.get("mode")
    if mode == "plan":
        return plan(event)
    if mode == "cluster":
        return run_cluster(event)
    failures = []
    for rec in event.get("Records", []):
        try:
            work(json.loads(rec["body"]))
        except Exception as e:
            print(f"[paket-fehler] {e!r}")
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}
