#!/usr/bin/env python3
"""Stand des S3-Imports:  AWS_PROFILE=photon python status.py [timemachine-2024-12-14]"""
import sys, boto3
name = sys.argv[1] if len(sys.argv) > 1 else "timemachine-2024-12-14"
ses = boto3.Session(region_name="eu-west-1")
it = ses.resource("dynamodb").Table("photonestudios-photos").get_item(Key={"sha256": f"__s3import__:{name}"}).get("Item")
if not it:
    sys.exit("noch kein Plan gelaufen")
f = int(it["already_files"]) + int(it["done_files"]); b = int(it["already_bytes"]) + int(it["done_bytes"])
print(f"{name} – geplant {it['planned_at']}, zuletzt {it['updated_at']}")
print(f"  {f}/{it['total_files']} Dateien | {b/1e9:.1f}/{int(it['total_bytes'])/1e9:.1f} GB = {100*b/int(it['total_bytes']):.1f} %")
print(f"  dieser Lauf: neu {it['new']} | doppelt {it['dup']} | skip {it['skip']} | Fehler {it['err']} | Pakete {it['packages_done']}/{it['packages']}")
try:
    sqs = ses.client("sqs")
    for q in ("photon-s3import", "photon-s3import-dlq"):
        u = sqs.get_queue_url(QueueName=q)["QueueUrl"]
        a = sqs.get_queue_attributes(QueueUrl=u, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        print(f"  {q}: wartend {a['ApproximateNumberOfMessages']}, in Arbeit {a['ApproximateNumberOfMessagesNotVisible']}")
except Exception as e:
    print(f"  (Queue-Status nicht lesbar: {e.__class__.__name__})")
