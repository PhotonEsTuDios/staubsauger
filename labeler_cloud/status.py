#!/usr/bin/env python3
"""Stand des Cloud-Labelers (staubsauger#6):  AWS_PROFILE=photon-new python status.py"""
import os, boto3
ses = boto3.Session(region_name="eu-west-1")
it = ses.resource("dynamodb").Table(os.environ.get("PHOTON_TABLE", "photonestudios-photos")).get_item(Key={"sha256": "__labeler__"}).get("Item")
if not it:
    raise SystemExit("noch kein Plan gelaufen")
done = int(it["labeled"]) + int(it["posters"]) + int(it["err"])
tot = int(it["total"]) or 1
print(f"Plan {it['planned_at']}, zuletzt {it['updated_at']}")
print(f"  {done}/{it['total']} = {100*done/tot:.1f} %  | Fotos gelabelt {it['labeled']}/{it['photos']} | Poster {it['posters']}/{it['videos']} | EXIF ergänzt {it['exif_filled']} | Fehler {it['err']}")
print(f"  Pakete {it['packages_done']}/{it['packages']}")
sqs = ses.client("sqs")
for q in ("photon-labeler", "photon-labeler-dlq"):
    u = sqs.get_queue_url(QueueName=q)["QueueUrl"]
    a = sqs.get_queue_attributes(QueueUrl=u, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    print(f"  {q}: wartend {a['ApproximateNumberOfMessages']}, in Arbeit {a['ApproximateNumberOfMessagesNotVisible']}")
