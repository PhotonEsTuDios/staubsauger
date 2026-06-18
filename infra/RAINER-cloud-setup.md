# Für Rainer — Cloud-Setup Dropbox-Ingest

Die **lokale** Staubsauger-Pipeline läuft bereits (S3 + DynamoDB, Policy `iam-user-policy.json`
ist gesetzt). Für den **Dropbox-Ingest in der Cloud** (Lambda zieht direkt von Dropbox)
brauchen wir noch folgendes — alles in **eu-west-1**, Account **030760499811**:

## 1. Secrets Manager
Secret `photonestudios/dropbox` (JSON):
```json
{ "app_key": "…", "app_secret": "…", "refresh_token": "…" }
```
(Werte liefert Daniela aus der Dropbox-App — siehe `dropbox_cloud/README.md`.)

## 2. IAM-Rolle für die Lambda: `photon-dropbox-ingest-role`
Trust Policy: `lambda.amazonaws.com`. Permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::photonestudios-photos/*" },
    { "Effect": "Allow", "Action": ["dynamodb:GetItem","dynamodb:PutItem"],
      "Resource": "arn:aws:dynamodb:eu-west-1:030760499811:table/photonestudios-photos" },
    { "Effect": "Allow", "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:eu-west-1:030760499811:secret:photonestudios/dropbox-*" },
    { "Effect": "Allow", "Action": ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],
      "Resource": "arn:aws:logs:eu-west-1:030760499811:*" }
  ]
}
```

## 3. (Optional) Deploy-Rechte für Danielas User
Falls Daniela die Lambda selbst deployen/aktualisieren soll, braucht `daniela-haering`
zusätzlich: `lambda:CreateFunction`, `lambda:UpdateFunctionCode`, `lambda:GetFunction`,
`iam:PassRole` (nur auf `photon-dropbox-ingest-role`), `events:PutRule`, `events:PutTargets`.
Sonst deployst du es einmalig und gut.

## 4. EventBridge-Zeitplan
Regel `photon-dropbox-hourly` → `rate(1 hour)` → Target = Lambda `photon-dropbox-ingest`.

---
Deploy-Schritte & Funktionsweise: `dropbox_cloud/README.md`.
