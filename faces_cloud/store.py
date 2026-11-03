"""Speicher für die Gesichtserkennung (staubsauger#14): DynamoDB + S3, sonst nichts.

Tabelle PHOTON_FACES_TABLE (PK sha256 S, SK face N), je Gesicht:
  box [x,y,w,h relativ], score, px, vec (float16-Binär), crop (S3-Key), cluster, person
Foto-Tabelle PHOTON_TABLE: face_version, face_count, faces_at, face_clusters, persons
S3: faces/<sha>_<i>.jpg (Ausschnitt), faces/clusters.json (Übersicht für die Gallery)
"""
import datetime as dt
import json
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config

BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-archive")
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
FACES_TABLE = os.environ.get("PHOTON_FACES_TABLE", "photonestudios-faces")
FACE_VERSION = 1
PROGRESS = {"sha256": "__faces__"}

_cfg = Config(connect_timeout=10, read_timeout=60, max_pool_connections=32,
              retries={"max_attempts": 8, "mode": "adaptive"})


class Store:
    def __init__(self, s3=None, photos=None, faces=None):
        ddb = None if (photos and faces) else boto3.resource("dynamodb", config=_cfg)
        self.s3 = s3 or boto3.client("s3", config=_cfg)
        self.photos = photos or ddb.Table(TABLE)
        self.faces = faces or ddb.Table(FACES_TABLE)

    @staticmethod
    def now():
        return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ---- Plan -----------------------------------------------------------------
    def photos_todo(self, exts, limit=0):
        kw = {"ProjectionExpression": "sha256, s3_key",
              "FilterExpression": Attr("s3_key").exists() & Attr("face_version").not_exists()
              & Attr("face_error").not_exists()}
        todo = []
        while True:
            r = self.photos.scan(**kw)
            for it in r.get("Items", []):
                sha, key = str(it["sha256"]), str(it.get("s3_key", ""))
                if not sha.startswith("__") and os.path.splitext(key)[1].lower() in exts:
                    todo.append({"sha": sha, "key": key})
            if "LastEvaluatedKey" not in r or (limit and len(todo) >= limit):
                break
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
        return todo[:limit] if limit else todo

    # ---- Worker ---------------------------------------------------------------
    def already_done(self, sha):
        it = self.photos.get_item(Key={"sha256": sha}, ProjectionExpression="face_version").get("Item")
        return bool(it and "face_version" in it)

    def read_original(self, key):
        return self.s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()

    def save_faces(self, sha, faces, vec_to_bytes):
        for i, f in enumerate(faces):
            crop_key = f"faces/{sha}_{i}.jpg"
            if f.get("crop"):
                self.s3.put_object(Bucket=BUCKET, Key=crop_key, Body=f["crop"], ContentType="image/jpeg")
            self.faces.put_item(Item={
                "sha256": sha, "face": i,
                "box": [Decimal(str(v)) for v in f["box"]],
                "score": Decimal(str(f["score"])), "px": f["px"],
                "vec": vec_to_bytes(f["vec"]), "crop": crop_key,
            })
        # Erst zuletzt am Foto markieren -> Abbruch vorher wird beim nächsten Plan wiederholt.
        self.photos.update_item(
            Key={"sha256": sha},
            UpdateExpression="SET face_version=:v, face_count=:n, faces_at=:t",
            ConditionExpression="attribute_not_exists(face_version)",
            ExpressionAttributeValues={":v": FACE_VERSION, ":n": len(faces), ":t": self.now()})

    def mark_error(self, sha, err):
        self.photos.update_item(Key={"sha256": sha}, UpdateExpression="SET face_error=:e, faces_at=:t",
                                ExpressionAttributeValues={":e": err[:300], ":t": self.now()})

    # ---- Fortschritt -----------------------------------------------------------
    def start_progress(self, **counts):
        self.photos.put_item(Item={**PROGRESS, "planned_at": self.now(), "updated_at": self.now(),
                                   "done": 0, "faces": 0, "err": 0, "packages_done": 0, **counts})

    def add_progress(self, done, faces, err):
        self.photos.update_item(
            Key=PROGRESS,
            UpdateExpression="ADD done :d, faces :f, err :e, packages_done :one SET updated_at=:t",
            ExpressionAttributeValues={":d": done, ":f": faces, ":e": err, ":one": 1, ":t": self.now()})

    # ---- Clustering ------------------------------------------------------------
    def all_faces(self, min_score=0.9, min_px=40):
        kw = {"ProjectionExpression": "sha256, face, vec, px, score, crop, #c, person",
              "ExpressionAttributeNames": {"#c": "cluster"}}
        out = []
        while True:
            r = self.faces.scan(**kw)
            for it in r.get("Items", []):
                if float(it["score"]) >= min_score and int(it["px"]) >= min_px:
                    out.append(it)
            if "LastEvaluatedKey" not in r:
                return out
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    def set_cluster(self, sha, face, cluster):
        if cluster:
            self.faces.update_item(Key={"sha256": sha, "face": face}, UpdateExpression="SET #c=:c",
                                   ExpressionAttributeNames={"#c": "cluster"},
                                   ExpressionAttributeValues={":c": cluster})
        else:
            self.faces.update_item(Key={"sha256": sha, "face": face}, UpdateExpression="REMOVE #c",
                                   ExpressionAttributeNames={"#c": "cluster"})

    def set_photo_people(self, sha, clusters, persons):
        names, vals, sets = {}, {}, []
        for attr, values in (("face_clusters", clusters), ("persons", persons)):
            if values:
                vals[f":{attr}"] = set(values)
                sets.append(f"{attr}=:{attr}")
        rem = [a for a, v in (("face_clusters", clusters), ("persons", persons)) if not v]
        expr = (("SET " + ", ".join(sets)) if sets else "") + ((" REMOVE " + ", ".join(rem)) if rem else "")
        kw = {"Key": {"sha256": sha}, "UpdateExpression": expr.strip()}
        if vals:
            kw["ExpressionAttributeValues"] = vals
        self.photos.update_item(**kw)

    def write_summary(self, summary):
        self.s3.put_object(Bucket=BUCKET, Key="faces/clusters.json", ContentType="application/json",
                           Body=json.dumps(summary, ensure_ascii=False).encode())
