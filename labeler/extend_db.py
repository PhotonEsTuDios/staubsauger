"""DB-Erweiterung fuer Auto-Labeling: legt einen dokumentierenden Schema-Marker
an. DynamoDB ist schemalos -> die echten Felder entstehen beim ersten UpdateItem
des Labelers. Dieser Marker haelt fest, WELCHE Felder ein gelabeltes Foto traegt.
"""
import os
import boto3

ddb = boto3.resource("dynamodb", region_name="eu-west-1").Table(os.environ.get("PHOTON_TABLE", "photonestudios-photos"))

SCHEMA = {
    "sha256": "__label_schema__",
    "label_version": 1,
    "note": "Auto-Labeling-Felder pro Foto (gesetzt vom labeler.py).",
    "fields": {
        "quality": "sharpness(int, Laplace-Varianz), brightness(0-255), contrast(int), quality_score(0-100), blurry(bool)",
        "color": "dominant_colors = Liste Hex-Strings (haeufigste zuerst)",
        "faces": "person_ids = Liste (spaeter, InsightFace+Clustering), face_count(int)",
        "tags": "labels = Liste freier Tags, scene = String (spaeter, Claude Vision)",
        "meta": "labeled_at(ISO), label_version(int), label_dims = welche Dimensionen gelaufen sind",
    },
}

ddb.put_item(Item=SCHEMA)
print("Schema-Marker __label_schema__ geschrieben:")
for k, v in SCHEMA["fields"].items():
    print(f"  {k}: {v}")
