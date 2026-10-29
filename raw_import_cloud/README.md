# Raw-Import (S3-Rohspiegel → Archiv)

Importiert einen Rohspiegel unter `s3://photonestudios-photos/raw/<name>/` (z.B. das
Time-Machine-Backup, hochgeladen mit `../raw_mirror.py`) ins Archiv — gleiches
Prinzip wie der Dropbox-Staubsauger:

stream → SHA-256 → Dedup gegen DynamoDB → `<sha>.<ext>` in S3 + DynamoDB-Item.

* **Neu:** Original nach `<sha>.<ext>`, Item mit `source`, `sources=[source]`,
  `source_path` (relativer Originalpfad), `raw_key`, `uploaded_at` = Datei-mtime,
  Metadaten (Datum, Kamera, Objektiv, ISO, Blende, Belichtung, Brennweite, Maße, GPS).
* **Duplikat:** nichts hochladen; `sources` um die neue Herkunft ergänzen, fehlende
  Metadaten nachtragen (nie überschreiben), Pfad in `alt_paths`.
* **Zips** (Dropbox-Exporte) werden per HTTP-Range gelesen und Mitglied für
  Mitglied importiert (`source_path` = `zip!mitglied`).

Die Gallery filtert nach Herkunft (`sources`/`source` → Dropbox / Time Machine /
iCloud / Lokal) und zeigt den Fortschritt unter **/import**.

## Aufbau

| Teil | Wo | Aufgabe |
|---|---|---|
| `raw_import.py`, `meta.py` | Lambda `photon-dropbox-ingest`, Modus `raw-import` / `raw-zip-list` | eigentliche Arbeit |
| `driver.py` | Mac | Arbeitsliste, Batches, presigned URLs, Buchführung, Fortschritt |
| `status.py` | Mac | Stand auf der Konsole |
| `deploy.sh` | Mac | Lambda-Zip bauen (Dropbox-Handler + Raw-Import + exifread) und deployen |

Warum in der Dropbox-Lambda und mit presigned URLs: Danielas IAM-User darf keine
Funktionen/Rollen anlegen, die Rolle `photon-dropbox-ingest-role` hat kein
`s3:GetObject`. `handler.py` verzweigt nur bei `event.mode`, ohne `mode` läuft der
Dropbox-Ingest unverändert. Lesen über presigned URLs in-Region kostet keinen Transfer.

**Sauberer (für Rainer):** eigene Funktion `photon-raw-import` + Rolle mit
`s3:GetObject` (raw/*), `s3:PutObject`, `dynamodb:GetItem/PutItem`; dann
`PHOTON_IMPORT_FUNCTION=photon-raw-import` für den Treiber setzen. Mit höherer
reserved concurrency könnte der Treiber parallel invoken.

## Bedienung

```bash
cd ~/PhotonEsTuDios/staubsauger/raw_import_cloud
PYTHON=~/PhotonEsTuDios/.venv/bin/python ./deploy.sh
AWS_PROFILE=photon ~/PhotonEsTuDios/.venv/bin/python driver.py raw/timemachine-2024-12-14/ --source timemachine:2024-12-14
AWS_PROFILE=photon ~/PhotonEsTuDios/.venv/bin/python status.py timemachine-2024-12-14
tail -f logs/timemachine-2024-12-14.log
```

Fortsetzbar: bereits erledigte Dateien stehen in `state/<name>/results.tsv` und
werden übersprungen; Fehler werden am Ende einmal wiederholt.
Fortschritt liegt zusätzlich in S3: `raw/<name>/_import/progress.json` und
`results.tsv` (Gallery-Seite `/import`).
