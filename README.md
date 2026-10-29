# PhotonEsTuDios — Staubsauger 🌀

**Foto-Archiv:** Bilder aus allen Quellen (Mac, Festplatte, Dropbox) flach nach
**AWS S3 (eu-west-1)** saugen und in **DynamoDB** indexieren.

Dedup über **SHA-256**: gleicher Bildinhalt = gleicher S3-Key = kein Doppel-Upload,
egal aus welcher Quelle. Der DynamoDB-Index ist das Inventar („was ist schon drin").

## Architektur

```
Mac / Festplatte ──(staubsauger.py, lokal)──┐
                                             ├──► S3: photonestudios-photos/<sha256>.<ext>  (flach)
Dropbox ─────────(Lambda, in der Cloud)──────┘        +
                                                   DynamoDB: photonestudios-photos  (PK = sha256)
```

| Komponente | Wert |
|---|---|
| Region | `eu-west-1` (Irland) |
| S3-Bucket | `photonestudios-photos` |
| DynamoDB-Tabelle | `photonestudios-photos` (PK `sha256`, On-Demand) |
| AWS-Profil | `photon` |
| Account | `030760499811` · IAM-User `daniela-haering` |

## DynamoDB-Schema (pro Bild)
`sha256` (PK), `s3_key`, `s3_bucket`, `original_filename`, `source_path`, `source`,
`taken_at`, `ym` (YYYY-MM), `gps_lat`, `gps_lon`, `camera_make`, `camera_model`,
`lens`, `iso`, `f_number`, `exposure`, `focal_length`, `width`, `height`, `bytes`,
`mime`, `uploaded_at`.

## Lokal saugen (Mac / Festplatte)

```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
brew install exiftool   # einmalig, für Metadaten (inkl. RAW/HEIC/Video)

# Probelauf (nichts wird hochgeladen):
python staubsauger.py ~/Photos/Claude-Workspace --source mac:claude-workspace --dry-run

# Echt, gedeckelt:
python staubsauger.py ~/Photos/Claude-Workspace --source mac:claude-workspace --limit 20

# Festplatte, nur ab Datum:
python staubsauger.py /Volumes/LaCie/Shoots --source drive:lacie --since 2026-01-01
```

Flags: `--source` (Pflicht, Quellen-Label) · `--profile` · `--region` · `--bucket`
· `--table` · `--since YYYY-MM-DD` · `--limit N` · `--dry-run`.

## Dropbox (in der Cloud, nicht über den Rechner)
Siehe **`dropbox_cloud/`** — Lambda zieht direkt von Dropbox. Secrets in AWS Secrets
Manager (`photonestudios/dropbox`), **nie im Repo**. Setup: `infra/RAINER-cloud-setup.md`.

## Status (Stand 19.06.2026)
- ✅ **Lokale Pipeline** fertig & getestet — Testballon mit ~40 Bildern aus
  `~/Photos/Claude-Workspace` gelaufen, Dedup verifiziert.
- ✅ **S3-Bucket + DynamoDB-Tabelle** angelegt und beschreibbar.
- 🔜 **Dropbox-Cloud-Ingest**: Code fertig (`dropbox_cloud/handler.py`), noch nicht
  deployt — braucht Dropbox-App (Daniela) + Lambda/Secrets-Rechte (Rainer).
- 🔜 **Volle EXIF in der Cloud** via exiftool-Lambda-Layer (optional).

---
*PhotonEsTuDios · Granada, Andalucía · in Zusammenarbeit mit LaMusicaEsTuDios*

## Konfiguration (Account-Umzug, infra#11)

Alle Skripte und Lambdas lesen Ziel-Bucket, Tabelle und Profil aus der Umgebung.
Ohne Angabe gelten die alten Werte, deshalb ändert sich am laufenden Betrieb nichts.

| Variable | Standard | Bedeutung |
|---|---|---|
| `PHOTON_BUCKET` | `photonestudios-photos` | Archiv-Bucket |
| `PHOTON_TABLE` | `photonestudios-photos` | DynamoDB-Index |
| `AWS_PROFILE` | `photon` | AWS-Profil auf dem Mac (`photon-new` = neuer Account) |

`s3importer/setup-aws.sh` ermittelt die Account-ID selbst aus dem aktiven Profil.
