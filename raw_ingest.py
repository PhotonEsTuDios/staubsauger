#!/usr/bin/env python3
"""Ingest aus einem S3-Rohspiegel (raw/<name>/) ins Archiv — beliebig oft wiederholbar.

Je Manifest-Eintrag: Datei aus S3 (oder lokal, falls source_path existiert) lesen,
SHA-256 + exiftool. Neuer SHA → S3-Kopie nach <sha>.<ext> + DynamoDB-Item.
  * normale Dateien: serverseitige copy_object (kein Re-Upload)
  * .zip: herunterladen, Medien-Einträge einzeln entpacken, neue hochladen
Doppelte SHAs werden nur gezählt. Ergebnis: result.tsv (lokal + neben Manifest).

  python raw_ingest.py raw/timemachine-2024-12-14/ --source timemachine:2024-12-14 [--dry-run] [--limit N] [--only-zips|--no-zips]
"""
import argparse, os, sys, tempfile, threading, time, zipfile, shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from staubsauger import (BUCKET, TABLE, REGION, IMAGE_EXTS, sha256_of,
                         read_metadata, build_item)

CFG = Config(connect_timeout=10, read_timeout=120, max_pool_connections=4,
             retries={"max_attempts": 8, "mode": "adaptive"})
_tl = threading.local()
_lock = threading.Lock()

def clients():
    """eigener Client je Thread (geteilter Client + TransferManager hat sich verklemmt)"""
    if not hasattr(_tl, "s3"):
        with _lock:
            sess = boto3.Session(profile_name="photon", region_name=REGION)
        _tl.s3 = sess.client("s3", config=CFG)
        _tl.table = sess.resource("dynamodb", config=CFG).Table(TABLE)
    return _tl.s3, _tl.table

def fetch(key, dest):
    s3, _ = clients()
    with open(dest, "wb") as fh:
        for chunk in s3.get_object(Bucket=BUCKET, Key=key)["Body"].iter_chunks(1 << 20):
            fh.write(chunk)

def register(path, ext, size, source, source_path, raw_key, dry, copy_from=None):
    """SHA prüfen, ggf. ins Archiv übernehmen. -> (sha, status)"""
    s3, table = clients()
    sha = sha256_of(path)
    if table.get_item(Key={"sha256": sha}, ProjectionExpression="sha256").get("Item"):
        return sha, "dup"
    item = build_item(sha, source_path, source, ext, size, read_metadata(path))
    item["raw_key"] = raw_key
    if dry:
        return sha, "new"
    meta = {"original-filename": item["original_filename"].encode("ascii", "replace").decode(),
            "source": source}
    ctype = item.get("mime", "application/octet-stream")
    if copy_from and size < 5 * 1024**3:
        s3.copy_object(Bucket=BUCKET, Key=item["s3_key"], CopySource={"Bucket": BUCKET, "Key": copy_from},
                       ContentType=ctype, Metadata=meta, MetadataDirective="REPLACE")
    else:
        s3.upload_file(path, BUCKET, item["s3_key"], ExtraArgs={"ContentType": ctype, "Metadata": meta},
                       Config=TransferConfig(use_threads=False))
    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(sha256)")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException": raise
        return sha, "dup"
    return sha, "new"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prefix")
    ap.add_argument("--source", required=True)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--only-zips", action="store_true")
    g.add_argument("--no-zips", action="store_true")
    ap.add_argument("--tmp", default=tempfile.gettempdir())
    a = ap.parse_args()
    prefix = a.prefix.rstrip("/") + "/"
    s3, _ = clients()

    body = s3.get_object(Bucket=BUCKET, Key=prefix + "manifest.tsv")["Body"].read().decode()
    rows = [l.split("\t") for l in body.splitlines()[1:] if l.strip()]
    files = [r for r in rows if not r[0].lower().endswith(".zip")]
    zips = [r for r in rows if r[0].lower().endswith(".zip")]
    if a.only_zips: files = []
    if a.no_zips: zips = []
    if a.limit: files = files[:a.limit]; zips = zips[:max(0, a.limit - len(files))] if a.limit <= len(files) else zips
    print(f"{len(files)} Dateien + {len(zips)} Zips", flush=True)

    c = {"new": 0, "dup": 0, "err": 0, "skip_ext": 0}
    results = []; t0 = time.time()

    def done(raw_key, sha, st, total):
        with _lock:
            c[st] += 1; results.append(f"{raw_key}\t{sha}\t{st}")
            n = len(results)
            if n % 500 == 0 or n == total:
                print(f"{time.strftime('%H:%M')} {n}/{total} | neu {c['new']} | doppelt {c['dup']} | "
                      f"Fehler {c['err']} | {n/(time.time()-t0):.1f}/s", flush=True)

    def one_file(row, total):
        key, size, _mt, src = row
        ext = os.path.splitext(key)[1].lower()
        if ext not in IMAGE_EXTS:
            return done(key, "", "skip_ext", total)
        tmp = None
        try:
            if os.path.exists(src):
                path = src
            else:
                fd, tmp = tempfile.mkstemp(suffix=ext, dir=a.tmp); os.close(fd)
                fetch(key, tmp); path = tmp
            sha, st = register(path, ext, int(size), a.source, src, key, a.dry_run, copy_from=key)
            done(key, sha, st, total)
        except Exception as e:
            print(f"ERR {key}: {e}", file=sys.stderr, flush=True); done(key, "", "err", total)
        finally:
            if tmp and os.path.exists(tmp): os.remove(tmp)

    with ThreadPoolExecutor(a.workers) as ex:
        for f in as_completed([ex.submit(one_file, r, len(files)) for r in files]): f.result()

    for key, size, _mt, src in zips:
        zdir = tempfile.mkdtemp(prefix="zip-", dir=a.tmp)
        try:
            zpath = os.path.join(zdir, "a.zip"); print(f"Zip {key} ({int(size)/1e9:.1f} GB) laden …", flush=True)
            fetch(key, zpath)
            zf = zipfile.ZipFile(zpath)
            members = [m for m in zf.infolist() if not m.is_dir()
                       and os.path.splitext(m.filename)[1].lower() in IMAGE_EXTS
                       and not os.path.basename(m.filename).startswith(".")]
            print(f"  {len(members)} Medien im Zip", flush=True)
            def one_member(m):
                ext = os.path.splitext(m.filename)[1].lower()
                fd, tmp = tempfile.mkstemp(suffix=ext, dir=zdir); os.close(fd)
                rk = f"{key}!{m.filename}"
                try:
                    with zipfile.ZipFile(zpath) as z2, z2.open(m) as src_f, open(tmp, "wb") as dst:
                        shutil.copyfileobj(src_f, dst, 1 << 20)
                    sha, st = register(tmp, ext, m.file_size, a.source, f"{src}!{m.filename}", rk, a.dry_run)
                    done(rk, sha, st, len(members))
                except Exception as e:
                    print(f"ERR {rk}: {e}", file=sys.stderr, flush=True); done(rk, "", "err", len(members))
                finally:
                    os.remove(tmp)
            with ThreadPoolExecutor(a.workers) as ex:
                for f in as_completed([ex.submit(one_member, m) for m in members]): f.result()
        except Exception as e:
            print(f"ERR Zip {key}: {e}", file=sys.stderr, flush=True); done(key, "", "err", 1)
        finally:
            shutil.rmtree(zdir, ignore_errors=True)

    out = "raw_key\tsha256\tstatus\n" + "\n".join(sorted(results)) + "\n"
    name = "result-dry.tsv" if a.dry_run else "result.tsv"
    open(name, "w").write(out)
    if not a.dry_run:
        s3.put_object(Bucket=BUCKET, Key=prefix + name, Body=out.encode())
    print(f"FERTIG {'(DRY-RUN) ' if a.dry_run else ''}{c}", flush=True)

if __name__ == "__main__":
    main()
