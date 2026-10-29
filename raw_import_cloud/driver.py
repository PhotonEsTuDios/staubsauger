#!/usr/bin/env python3
"""Treiber für den Raw-Import (läuft auf dem Mac, die Arbeit passiert in der Lambda).

Liest manifest.tsv des Rohspiegels, listet Zip-Inhalte (per Lambda), schickt die
Dateien in Batches mit presigned URLs an die Lambda (Modus raw-import) und führt
Buch. Fortsetzbar: erledigte Keys (state/<name>/results.tsv) werden übersprungen,
Fehler am Ende einmal wiederholt.

Fortschritt — jederzeit:
  * S3  raw/<name>/_import/progress.json   (Gallery-Seite /import zeigt ihn an)
  * S3  raw/<name>/_import/results.tsv     (alle 10 Batches)
  * lokal  logs/<name>.log   und   python status.py <name>

  AWS_PROFILE=photon python driver.py raw/timemachine-2024-12-14/ --source timemachine:2024-12-14
"""
import argparse, json, os, time
import boto3
from botocore.config import Config

B = "photonestudios-photos"
FN = os.environ.get("PHOTON_IMPORT_FUNCTION", "photon-dropbox-ingest")
HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prefix")
    ap.add_argument("--source", required=True)
    ap.add_argument("--batch", type=int, default=120)
    ap.add_argument("--batch-gb", type=float, default=15.0)
    ap.add_argument("--limit", type=int, default=0, help="nur N Einheiten (Test)")
    a = ap.parse_args()
    prefix = a.prefix.rstrip("/") + "/"
    name = prefix.rstrip("/").split("/")[-1]
    sess = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "photon"), region_name="eu-west-1")
    s3 = sess.client("s3")
    lam = sess.client("lambda", config=Config(read_timeout=960, connect_timeout=10, retries={"max_attempts": 0}))
    sdir = os.path.join(HERE, "state", name); os.makedirs(sdir, exist_ok=True)
    os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)
    logf = open(os.path.join(HERE, "logs", f"{name}.log"), "a", buffering=1)

    def log(msg):
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
        print(line, flush=True); logf.write(line + "\n")

    def presign(key):
        return s3.generate_presigned_url("get_object", Params={"Bucket": B, "Key": key}, ExpiresIn=6 * 3600)

    def invoke(payload):
        for attempt in range(60):
            try:
                r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
                body = json.loads(r["Payload"].read() or b"{}")
                if r.get("FunctionError"):
                    raise RuntimeError(f"FunctionError: {str(body)[:400]}")
                return body
            except lam.exceptions.TooManyRequestsException:
                log("Lambda belegt (Dropbox-Lauf?) – warte 30 s"); time.sleep(30)
            except Exception as e:
                log(f"Invoke-Fehler ({attempt+1}): {e!r}"[:500]); time.sleep(min(60, 5 * (attempt + 1)))
        raise RuntimeError("Lambda dauerhaft nicht erreichbar")

    # ---- Arbeitsliste ----------------------------------------------------
    rows = [l.split("\t") for l in s3.get_object(Bucket=B, Key=prefix + "manifest.tsv")["Body"].read().decode().splitlines()[1:] if l.strip()]
    rel = lambda src: src.split("/Users/potstupimi/", 1)[-1]
    units = []
    for key, size, mtime, src in rows:
        if not key.lower().endswith(".zip"):
            units.append({"key": key, "size": int(size), "mtime": int(mtime), "path": rel(src)})
    for key, size, mtime, src in rows:
        if key.lower().endswith(".zip"):
            cache = os.path.join(sdir, "zip-" + key.replace("/", "_") + ".json")
            if os.path.exists(cache):
                members = json.load(open(cache))
            else:
                members = invoke({"mode": "raw-zip-list", "url": presign(key), "size": int(size)})["members"]
                json.dump(members, open(cache, "w"))
            log(f"Zip {key.split('/')[-1]}: {len(members)} Medien")
            for m in members:
                units.append({"key": f"{key}!{m['member']}", "size": int(m["size"]), "mtime": int(m["mtime"]),
                              "path": f"{rel(src)}!{m['member']}", "member": m["member"],
                              "zip_key": key, "zip_size": int(size)})
    if a.limit:
        units = units[:a.limit]
    total_n, total_b = len(units), sum(u["size"] for u in units)
    size_of = {u["key"]: u["size"] for u in units}

    resf = os.path.join(sdir, "results.tsv")
    done = {}
    if os.path.exists(resf):
        for l in open(resf):
            p = l.rstrip("\n").split("\t")
            if len(p) >= 3:
                done[p[0]] = p
    log(f"Start: {total_n} Einheiten, {total_b/1e9:.1f} GB, schon erledigt {sum(1 for p in done.values() if p[2] != 'err')}")

    t0 = time.time()
    st = {"bytes": 0, "batches": 0}
    hang_count = {}

    def write_progress(state, current=""):
        ok = [k for k, p in done.items() if p[2] != "err" and k in size_of]
        done_b = sum(size_of[k] for k in ok)
        el = time.time() - t0
        rate = st["bytes"] / el if el > 0 else 0
        prog = {"name": name, "source": a.source, "state": state,
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t0)),
                "total_files": total_n, "done_files": len(ok), "total_bytes": total_b, "done_bytes": done_b,
                "percent": round(100 * done_b / total_b, 1) if total_b else 100.0,
                "counts": {k: sum(1 for p in done.values() if p[2] == k) for k in ("new", "dup", "skip", "err")},
                "rate_mb_s": round(rate / 1e6, 1),
                "eta_h": round((total_b - done_b) / rate / 3600, 2) if rate else None,
                "current": current, "batches": st["batches"],
                "last_errors": [{"key": k, "error": p[3] if len(p) > 3 else ""} for k, p in done.items() if p[2] == "err"][-15:]}
        s3.put_object(Bucket=B, Key=prefix + "_import/progress.json", Body=json.dumps(prog, indent=1, ensure_ascii=False).encode(),
                      ContentType="application/json", CacheControl="no-store")
        return prog

    def flush_results():
        s3.put_object(Bucket=B, Key=prefix + "_import/results.tsv",
                      Body=("raw_key\tsha256\tstatus\terror\n" + "".join("\t".join(p) + "\n" for p in done.values())).encode())

    def run(todo, label, per_batch):
        i = 0
        while i < len(todo):
            batch, bb = [], 0
            while i < len(todo) and len(batch) < per_batch and (bb < a.batch_gb * 1e9 or not batch):
                u = todo[i]; i += 1
                it = dict(u)
                if u.get("member"):
                    it["zip_url"] = presign(u["zip_key"])
                else:
                    it["url"] = presign(u["key"])
                batch.append(it); bb += u["size"]
            while batch:
                write_progress("läuft", f"{label}: Batch {st['batches']+1} mit {len(batch)} Dateien ({bb/1e9:.1f} GB)")
                res = invoke({"mode": "raw-import", "source": a.source, "items": batch})
                got = {r["key"]: r for r in res.get("results", [])}
                with open(resf, "a") as f:
                    for k, r in got.items():
                        p = [k, r.get("sha", ""), r["status"], (r.get("error") or "").replace("\t", " ").replace("\n", " ")]
                        done[k] = p; f.write("\t".join(p) + "\n")
                        if r["status"] != "err":
                            st["bytes"] += size_of.get(k, 0)
                st["batches"] += 1
                for k in res.get("hung", []):  # 2x hängend -> als Fehler verbuchen, nicht endlos wiederholen
                    hang_count[k] = hang_count.get(k, 0) + 1
                    if hang_count[k] >= 2:
                        p = [k, "", "err", "hängt in der Lambda (2x Timeout)"]
                        got[k] = {"key": k, "status": "err"}; done[k] = p
                        with open(resf, "a") as f:
                            f.write("\t".join(p) + "\n")
                if res.get("hung"):
                    log(f"hängend: {res['hung'][:5]}")
                pend = set(res.get("pending", []))
                batch = [x for x in batch if x["key"] not in got]  # pending + verlorene nochmal
                bb = sum(x["size"] for x in batch)
                if batch and not got:
                    log(f"Batch ohne Ergebnis, {len(batch)} offen – kurze Pause"); time.sleep(20)
                prog = write_progress("läuft")
                c = prog["counts"]
                log(f"{label} Batch {st['batches']}: +{len(got)} | gesamt {prog['done_files']}/{total_n} "
                    f"({prog['percent']} % der Bytes) | neu {c['new']} doppelt {c['dup']} skip {c['skip']} Fehler {c['err']} | "
                    f"{prog['rate_mb_s']} MB/s | ETA {prog['eta_h']} h" + (f" | {len(batch)} offen" if batch else ""))
                if st["batches"] % 10 == 0:
                    flush_results()

    run([u for u in units if u["key"] not in done], "Lauf", a.batch)
    retry = [u for u in units if done.get(u["key"], ["", "", ""])[2] == "err"]
    if retry:
        log(f"Wiederhole {len(retry)} Fehler in kleinen Batches")
        run(retry, "Retry", 5)
    flush_results()
    prog = write_progress("fertig")
    log(f"FERTIG: {prog['counts']} | {prog['done_files']}/{total_n} Dateien")


if __name__ == "__main__":
    main()
