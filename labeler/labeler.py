"""PhotonEsTuDios Auto-Labeler — Worker-Pool ueber das Foto-Archiv.

Erste Ausbaustufe: Qualitaet (Schaerfe/Helligkeit/Kontrast) + Farbe (Dominanzfarben).
Nur Pillow+numpy, kein Cloud-Call, kein API-Key. Faces/Tags kommen als Module nach.

Idempotent + resumierbar: jedes Foto wird einzeln per UpdateItem committet und per
`attribute_not_exists(label_version)` uebersprungen, wenn schon gelabelt. Abbruch
(SIGTERM/Ctrl-C) ist daher unkritisch.

  AWS_PROFILE=photon python labeler.py [--limit N] [--workers N]
"""
import io
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor

import boto3
from PIL import Image

from quality import LABEL_VERSION, analyze

REGION = "eu-west-1"
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")
WORKERS = int(os.environ.get("PHOTON_LABEL_WORKERS", "8"))
# Formate, die Pillow ohne Zusatz-Plugin sicher oeffnet (HEIC/RAW spaeter):
OK_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff", ".tif"}

ddb = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
s3 = boto3.client("s3", region_name=REGION)

_lock = threading.Lock()
counts = {"labeled": 0, "skipped": 0, "errors": 0}
_t0 = time.time()


def process(item: dict) -> str:
    sha = item["sha256"]
    key = item["s3_key"]
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        img = Image.open(io.BytesIO(body))
        m = analyze(img)
        ddb.update_item(
            Key={"sha256": sha},
            UpdateExpression=(
                "SET sharpness=:s, brightness=:b, contrast=:c, quality_score=:q, "
                "blurry=:bl, dominant_colors=:dc, labeled_at=:t, "
                "label_version=:v, label_dims=:d"
            ),
            ExpressionAttributeValues={
                ":s": m["sharpness"], ":b": m["brightness"], ":c": m["contrast"],
                ":q": m["quality_score"], ":bl": m["blurry"],
                ":dc": m["dominant_colors"],
                ":t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                ":v": LABEL_VERSION, ":d": ["quality", "color"],
            },
        )
        return "labeled"
    except Exception as e:  # ein kaputtes Bild darf den Lauf nicht kippen
        if os.environ.get("PHOTON_LABEL_DEBUG"):
            print(f"  err {key}: {e!r}", file=sys.stderr)
        return "errors"


def tally(result: str):
    with _lock:
        counts[result] += 1
        done = counts["labeled"]
        if done and done % 25 == 0 and result == "labeled":
            rate = done / max(1e-9, time.time() - _t0) * 3600
            print(f"  {done} gelabelt  ({rate:,.0f}/h)  err={counts['errors']}", flush=True)


def main():
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    print(f"Labeler startet — {WORKERS} Worker, Felder: quality+color", flush=True)

    scan_kw = {
        "ProjectionExpression": "sha256, s3_key",
        "FilterExpression": "attribute_exists(s3_key) AND attribute_not_exists(label_version)",
    }
    seen = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while True:
            resp = ddb.scan(**scan_kw)
            batch = []
            for it in resp.get("Items", []):
                if str(it["sha256"]).startswith("__"):
                    continue
                ext = os.path.splitext(it.get("s3_key", ""))[1].lower()
                if ext not in OK_EXTS:
                    counts["skipped"] += 1
                    continue
                batch.append(it)
            for r in pool.map(process, batch):
                tally(r)
            seen += len(batch)
            if limit and counts["labeled"] >= limit:
                break
            if "LastEvaluatedKey" not in resp:
                break
            scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    dt = time.time() - _t0
    print(f"\nFertig/abgebrochen nach {dt:.0f}s — "
          f"gelabelt={counts['labeled']} skip={counts['skipped']} err={counts['errors']}",
          flush=True)


if __name__ == "__main__":
    main()
