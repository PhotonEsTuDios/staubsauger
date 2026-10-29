#!/usr/bin/env python3
"""Rohspiegel: lädt Dateien aus einer Liste 1:1 (mit Ordnerstruktur) nach S3.

Fortsetzbar: bereits vorhandene Keys mit gleicher Größe werden übersprungen.
Am Ende liegt manifest.tsv (key, bytes, mtime, originalpfad) unter dem Prefix.

  python raw_mirror.py LISTE.txt --root ROOT --prefix raw/timemachine-2024-12-14/
"""
import argparse, os, sys, time, threading, io
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from boto3.s3.transfer import TransferConfig

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("listfile")
    ap.add_argument("--root", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--part-concurrency", type=int, default=4, help="parallele Teile je großer Datei (USB-2-Platte: 1-2)")
    a = ap.parse_args()
    root = a.root.rstrip("/") + "/"
    prefix = a.prefix.rstrip("/") + "/"
    s3 = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "photon"), region_name="eu-west-1").client("s3")
    cfg = TransferConfig(multipart_threshold=64 * 1024**2, max_concurrency=a.part_concurrency)

    paths = [l.rstrip("\n") for l in open(a.listfile, encoding="utf-8") if l.strip()]
    existing = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            existing[o["Key"]] = o["Size"]

    rows, todo = [], []
    for p in paths:
        if not p.startswith(root):
            print("ausserhalb root:", p, file=sys.stderr); continue
        st = os.stat(p)
        key = prefix + p[len(root):]
        rows.append(f"{key}\t{st.st_size}\t{int(st.st_mtime)}\t{p}")
        if existing.get(key) != st.st_size:
            todo.append((p, key, st.st_size))

    total_b = sum(t[2] for t in todo)
    print(f"{len(paths)} Dateien, {len(paths)-len(todo)} schon da, {len(todo)} zu laden ({total_b/1e9:.1f} GB)", flush=True)
    done_n = done_b = err = 0
    lock = threading.Lock(); t0 = time.time()

    def up(item):
        p, key, size = item
        for attempt in range(5):
            try:
                s3.upload_file(p, BUCKET, key, Config=cfg)
                return size
            except Exception as e:
                if attempt == 4:
                    print(f"FEHLER {p}: {e}", file=sys.stderr, flush=True); raise
                time.sleep(2 ** attempt * 3)

    with ThreadPoolExecutor(a.workers) as ex:
        futs = [ex.submit(up, t) for t in todo]
        for f in as_completed(futs):
            with lock:
                try: done_b += f.result(); done_n += 1
                except Exception: err += 1
                if (done_n + err) % 200 == 0 or done_n + err == len(todo):
                    el = time.time() - t0; rate = done_b / el if el else 0
                    eta = (total_b - done_b) / rate / 3600 if rate else 0
                    print(f"{time.strftime('%H:%M')} {done_n}/{len(todo)} | {done_b/1e9:.1f}/{total_b/1e9:.1f} GB | "
                          f"{rate*8/1e6:.0f} Mbit/s | ETA {eta:.1f} h | Fehler {err}", flush=True)

    s3.put_object(Bucket=BUCKET, Key=prefix + "manifest.tsv",
                  Body=("key\tbytes\tmtime\tsource_path\n" + "\n".join(rows) + "\n").encode())
    print(f"FERTIG: {done_n} geladen, {err} Fehler, manifest.tsv geschrieben", flush=True)

if __name__ == "__main__":
    main()
