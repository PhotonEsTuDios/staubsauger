#!/usr/bin/env python3
"""Stand iCloud-Import (staubsauger#2):  AWS_PROFILE=photon-new ~/PhotonEsTuDios/.venv/bin/python icloud_cloud/status.py"""
import boto3

R = "eu-west-1"
t = boto3.resource("dynamodb", region_name=R).Table("photonestudios-photos")
p = t.get_item(Key={"sha256": "__icloud__"}).get("Item")
if not p:
    raise SystemExit("Noch kein Lauf.")
g = lambda k: int(p.get(k, 0))  # noqa: E731
print(f"Status   {p.get('status')}  ({'Dry-Run' if p.get('dry_run') else 'echt'}, Lauf {p.get('run_id')}, Stand {p.get('status_at')})")
if p.get("status") == "login_needed":
    print("⚠ iCloud-Login nötig:  AWS_PROFILE=photon-new ~/PhotonEsTuDios/.venv/bin/python icloud_cloud/login.py")
tot = g("total")
print(f"Gelistet {g('offset')}/{tot}" + (f" ({100 * g('offset') / tot:.0f} %)" if tot else ""))
print(f"Schon da {g('known')} · Treffer im Archiv {g('matched')} (Name {g('match_name')}, Zeit+Maße {g('match_time_dims')}, "
      f"nur Zeit {g('match_time')}) · neu {g('new')} · GPS-Gewinn {g('gps_gain')}")
print(f"Worker   hochgeladen {g('uploaded')} · Duplikat {g('duplicate')} · ergänzt {g('enriched')} · fehlt {g('enrich_missing')}")
sqs = boto3.client("sqs", region_name=R)
for q in ("photon-icloud", "photon-icloud-dlq"):
    try:
        url = sqs.get_queue_url(QueueName=q)["QueueUrl"]
        a = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        print(f"Queue    {q}: wartend {a['ApproximateNumberOfMessages']}, in Arbeit {a['ApproximateNumberOfMessagesNotVisible']}")
    except sqs.exceptions.QueueDoesNotExist:
        pass
