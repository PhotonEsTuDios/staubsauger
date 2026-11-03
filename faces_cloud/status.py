#!/usr/bin/env python3
"""Stand der Gesichtserkennung (staubsauger#14):  AWS_PROFILE=photon-new python status.py [--cluster]"""
import json
import sys

import boto3
from botocore.exceptions import ClientError

ses = boto3.Session(region_name="eu-west-1")
it = ses.resource("dynamodb").Table("photonestudios-photos").get_item(Key={"sha256": "__faces__"}).get("Item")
if not it:
    raise SystemExit("noch kein Plan gelaufen")
done, err, tot = int(it["done"]), int(it["err"]), int(it["total"]) or 1
print(f"Plan {it['planned_at']}, zuletzt {it['updated_at']}")
print(f"  {done + err}/{it['total']} = {100 * (done + err) / tot:.1f} %  | Gesichter {it['faces']} | Fehler {err}"
      f" | Pakete {it['packages_done']}/{it['packages']}")
sqs = ses.client("sqs")
for q in ("photon-faces", "photon-faces-dlq"):
    u = sqs.get_queue_url(QueueName=q)["QueueUrl"]
    a = sqs.get_queue_attributes(QueueUrl=u, AttributeNames=["ApproximateNumberOfMessages",
                                                             "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    print(f"  {q}: wartend {a['ApproximateNumberOfMessages']}, in Arbeit {a['ApproximateNumberOfMessagesNotVisible']}")
if "--cluster" in sys.argv:
    try:
        s = json.loads(ses.client("s3").get_object(Bucket="photonestudios-archive", Key="faces/clusters.json")["Body"].read())
        cl = s["clusters"]
        print(f"Gruppen {s['generated_at']}: {len(cl)} aus {s['faces']} Gesichtern; größte: "
              + ", ".join(f"{c['cluster']}={c['size']}" for c in cl[:10]))
    except ClientError:
        print("noch nicht gruppiert")
