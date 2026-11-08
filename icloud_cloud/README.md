# iCloud-Fotos → Archiv (staubsauger#2)

Holt die iCloud-Mediathek **in der Cloud** ins Archiv (`photonestudios-archive` + DynamoDB), ohne Dropbox.
Apple hat keine Foto-API mit Key/Token → wir nutzen die iCloud-Web-Sitzung (pyicloud, fest gepinnt).

## Einmalig: Anmelden (Dani, ~2 Min., alle ~2 Monate wieder)
Voraussetzungen am iPhone: **Erweiterter Datenschutz AUS**, **„Auf iCloud-Daten im Web zugreifen“ AN**.
```bash
AWS_PROFILE=photon-new ~/PhotonEsTuDios/.venv/bin/python ~/PhotonEsTuDios/staubsauger/icloud_cloud/login.py
```
Apple-ID + Passwort + 2FA-Code selbst eintippen. Gespeichert wird **nur** die Sitzung (Cookies) im
Secrets Manager `photonestudios/icloud`, nie das Passwort. Läuft sie ab, zeigt `status.py` „Login nötig“.

## Ablauf
- `photon-icloud-plan` (1 gleichzeitig, einziger mit Secret-Zugriff): listet „Alle Fotos“, gleicht **ohne Download** ab
  (`matching.py`: Name+Bytes → Zeit+Maße → eindeutige Zeit). Treffer → nur GPS/Zeit/Favorit ergänzen,
  neu → Import-Paket. Setzt sich nach 10 Min. selbst fort.
- `photon-icloud-worker` (SQS, parallel): lädt per Download-Link, SHA-256-Dedup, S3 + DynamoDB `source=icloud`.
  Metadaten werden nur ergänzt (`if_not_exists`), nie überschrieben. GPS aus iCloud: `gps_source=icloud`.
- Fortschritt: DynamoDB-Eintrag `__icloud__`.

## Deploy / Läufe (nur per Skript)
```bash
cd ~/PhotonEsTuDios/staubsauger/icloud_cloud
AWS_PROFILE=photon-new PYTHON=~/PhotonEsTuDios/.venv/bin/python ./deploy.sh            # Dry-Run: nur zählen
AWS_PROFILE=photon-new PYTHON=… RUN=full LIMIT=50 ./deploy.sh                          # Pilot 50
AWS_PROFILE=photon-new PYTHON=… RUN=full SCHEDULE=1 ./deploy.sh                        # alles + alle 15 Min (#16)
AWS_PROFILE=photon-new ~/PhotonEsTuDios/.venv/bin/python status.py
```
Tests: `~/PhotonEsTuDios/.venv/bin/python -m pytest -q tests`

Offen: Live-Photo-Videoteil wird nicht geladen (nur das Standbild-Original); HEIC-Vorschau in der Gallery siehe Labeler.
