#!/usr/bin/env python3
"""Stand des Raw-Imports:  AWS_PROFILE=photon python status.py [timemachine-2024-12-14]"""
import json, os, sys
import boto3
name = sys.argv[1] if len(sys.argv) > 1 else "timemachine-2024-12-14"
s3 = boto3.Session(region_name="eu-west-1").client("s3")
p = json.loads(s3.get_object(Bucket=os.environ.get("PHOTON_BUCKET", "photonestudios-photos"), Key=f"raw/{name}/_import/progress.json")["Body"].read())
c = p["counts"]
print(f"{p['name']} [{p['state']}] Stand {p['updated_at']} (Start {p.get('started_at')})")
print(f"  {p['done_files']}/{p['total_files']} Dateien | {p['done_bytes']/1e9:.1f}/{p['total_bytes']/1e9:.1f} GB = {p['percent']} %")
print(f"  neu {c['new']} | doppelt {c['dup']} | übersprungen {c['skip']} | Fehler {c['err']}")
print(f"  {p['rate_mb_s']} MB/s | ETA {p['eta_h']} h | {p.get('current', '')}")
for e in p.get("last_errors", [])[-5:]:
    print(f"  ERR {e['key']}: {e['error'][:150]}")
