# S3-Importer (`photon-s3importer`)

Importiert einen Rohspiegel `s3://photonestudios-photos/raw/<name>/` (z.B. das
Time-Machine-Backup, hochgeladen mit `../raw_mirror.py`) ins Archiv —
**komplett in AWS**, parallel, ohne laufenden Rechner.

Gleiches Prinzip wie der Dropbox-Staubsauger: lesen → SHA-256 → Dedup gegen
DynamoDB → Original nach `<sha>.<ext>` + DynamoDB-Item mit Metadaten
(Datum, Kamera, Objektiv, ISO, Blende, Belichtung, Brennweite, Maße, GPS —
Bilder via `exifread`, Videos via eigenem MOV/MP4-Parser in `meta.py`).

* **Neu:** serverseitige Kopie `raw/…` → `<sha>.<ext>` (Zip-Mitglieder: Upload),
  Item mit `source`, `sources=[source]`, `source_path`, `raw_key`, `uploaded_at`=mtime.
* **Duplikat:** `sources` ergänzen, fehlende Metadaten füllen (nie überschreiben), `alt_paths`.
* **Überspringen:** Der Plan liest alle `raw/<name>/_import/results/*.tsv`; was dort
  als `new|dup|skip` steht, wird nicht noch einmal angefasst. Ein erneuter Plan-Aufruf
  macht also nur den Rest (und Fehler) — beliebig oft wiederholbar.

## Ablauf

```
plan (manuell)  ──►  SQS photon-s3import  ──►  Lambda-Worker (bis 20 parallel)
                         │ 3 Fehlversuche                │
                         ▼                               ├─► S3 <sha>.<ext>
                   photon-s3import-dlq                   ├─► DynamoDB Item
                                                         ├─► raw/<name>/_import/results/<msg>.tsv
                                                         └─► DynamoDB __s3import__:<name> (Zähler)
```

Pakete à 25 Dateien / max. 3 GB, Zip-Mitglieder je Zip gebündelt. Zeitbudget je
Aufruf: Unfertiges geht als neues Paket zurück (max. 3 Versuche je Datei, dann Fehler).

## Einrichten (einmalig, Admin)

```bash
AWS_PROFILE=<admin> PYTHON=~/PhotonEsTuDios/.venv/bin/python ./setup-aws.sh
```
Legt Queue + DLQ, Rolle `photon-s3importer-role`, Funktion (Python 3.12, arm64,
3 GB, 900 s, 10 GB /tmp), SQS-Trigger (MaximumConcurrency 20) und die
Betriebsrechte für `daniela-haering` an, schaltet den Dropbox-Zeitplan ab und
**startet den Time-Machine-Import**.

## Betrieb (User `photon`)

```bash
./deploy.sh                                   # Code-Update
python status.py timemachine-2024-12-14       # Stand + Queue-Längen
aws lambda invoke --function-name photon-s3importer --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  --payload '{"mode":"plan","prefix":"raw/timemachine-2024-12-14/","source":"timemachine:2024-12-14"}' /dev/null
```
Parallelität: `aws lambda update-event-source-mapping --uuid … --scaling-config MaximumConcurrency=N`.
Gallery: Seite **/import** zeigt den Stand live.

Neue Quelle (z.B. iCloud-Export): mit `raw_mirror.py` nach `raw/<name>/` spiegeln,
Plan mit `source=icloud:<datum>` starten, `<name>` in `gallery/server/api/import.get.ts` eintragen.
