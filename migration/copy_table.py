"""DynamoDB-Tabelle 1:1 zwischen Accounts kopieren (infra#6). Idempotent: überschreibt per Key.
  ~/PhotonEsTuDios/.venv/bin/python copy_table.py [--src-profile photon] [--dst-profile photon-new] [--table photonestudios-photos]
"""
import argparse, boto3
from concurrent.futures import ThreadPoolExecutor
a = argparse.ArgumentParser()
a.add_argument("--src-profile", default="photon"); a.add_argument("--dst-profile", default="photon-new")
a.add_argument("--table", default="photonestudios-photos"); a.add_argument("--dst-table")
a.add_argument("--segments", type=int, default=8)
o = a.parse_args()
src = boto3.Session(profile_name=o.src_profile, region_name="eu-west-1").resource("dynamodb").Table(o.table)
dst = boto3.Session(profile_name=o.dst_profile, region_name="eu-west-1").resource("dynamodb").Table(o.dst_table or o.table)

def seg(i):
    n, kw = 0, dict(Segment=i, TotalSegments=o.segments)
    with dst.batch_writer(overwrite_by_pkeys=["sha256"]) as w:
        while True:
            r = src.scan(**kw)
            for it in r["Items"]:
                w.put_item(Item=it); n += 1
            if "LastEvaluatedKey" not in r: return n
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

with ThreadPoolExecutor(o.segments) as ex:
    total = sum(ex.map(seg, range(o.segments)))
print("kopiert:", total)
