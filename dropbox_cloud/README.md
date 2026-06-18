# Dropbox-Cloud-Ingest

Zieht Fotos **direkt von Dropbox → S3 + DynamoDB**, läuft als **AWS Lambda in
eu-west-1** (nicht über Danielas Rechner), zeitgesteuert per EventBridge.

> **Status:** Code fertig, **noch nicht deployt.** Es fehlen (a) eine Dropbox-App
> + Refresh-Token von Daniela und (b) erweiterte AWS-Rechte/Rolle von Rainer
> (siehe `../infra/RAINER-cloud-setup.md`).

## Secrets — niemals ins Repo

Dropbox-App-Key/-Secret + Refresh-Token liegen in **AWS Secrets Manager** unter
`photonestudios/dropbox` als JSON:

```json
{ "app_key": "…", "app_secret": "…", "refresh_token": "…" }
```

Die Lambda liest sie zur Laufzeit. Im Git landet **nichts** davon.

## Was Daniela tun muss (Dropbox-App anlegen)

1. https://www.dropbox.com/developers/apps → **Create app**
   - *Scoped access* → *Full Dropbox* (oder nur den Foto-Ordner) → Name z.B. `photon-staubsauger`.
2. Im Tab **Permissions**: `files.metadata.read` + `files.content.read` aktivieren → **Submit**.
3. Im Tab **Settings**: `App key` + `App secret` notieren.
4. Einen **Refresh-Token** erzeugen (einmalig, OAuth2 mit `token_access_type=offline`):
   ```
   https://www.dropbox.com/oauth2/authorize?client_id=APP_KEY&response_type=code&token_access_type=offline
   ```
   Code gegen Tokens tauschen:
   ```
   curl https://api.dropboxapi.com/oauth2/token \
     -d code=AUTH_CODE -d grant_type=authorization_code \
     -u APP_KEY:APP_SECRET
   ```
   → `refresh_token` aus der Antwort übernehmen.
5. Die drei Werte an Rainer / ins Secrets Manager (NICHT in den Chat, NICHT ins Repo).

## Deploy (durch jemanden mit Lambda-Rechten — siehe infra/)

```bash
cd dropbox_cloud
zip -r ../build/dropbox_ingest.zip handler.py
aws lambda create-function \
  --function-name photon-dropbox-ingest \
  --runtime python3.12 --handler handler.handler \
  --role arn:aws:iam::030760499811:role/photon-dropbox-ingest-role \
  --timeout 300 --memory-size 512 \
  --zip-file fileb://../build/dropbox_ingest.zip \
  --region eu-west-1
# Zeitsteuerung (stündlich):
aws events put-rule --name photon-dropbox-hourly --schedule-expression "rate(1 hour)" --region eu-west-1
# + put-targets auf die Lambda
```

## Funktionsweise
- Holt kurzlebiges Access-Token aus dem Refresh-Token (Secrets Manager).
- `files/list_folder` **rekursiv**, danach **inkrementell per Cursor** (Cursor wird in
  DynamoDB unter `sha256="__dropbox_cursor__"` gespeichert → spätere Läufe sehen nur Neues).
- Pro Datei: download → SHA-256 → Dedup-Check (DynamoDB) → S3 `put_object` → `put_item`.
- **Dedup ist quellenübergreifend:** ein Bild, das schon vom Mac kam, wird nicht doppelt geladen.

> Hinweis Metadaten: in der Cloud kommt vorerst nur Basis-Metadaten aus der Dropbox-API
> (`time_taken`, Größe). Volle EXIF (GPS/Kamera) wie lokal via exiftool → optional als
> Lambda-Layer nachrüstbar.
