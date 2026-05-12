# PhotonEsTuDios — Staubsauger 🌀

**Dein persönlicher Foto-Sync:** Dropbox + iPhone/iPhoto → AWS S3 Cloud.

Alle deine Fotos automatisch in die Cloud. Einmal einrichten, läuft immer.

## Für deine Claude-Session

> "Ich bin Daniela Haering. Öffne den Staubsauger aus dem PhotonEsTuDios Repo. Ich möchte meine Fotos von Dropbox und iPhone nach S3 synchronisieren. Mein AWS-Profil ist `photon`."

## Was der Staubsauger macht

```
Dropbox/Fotos/     ──┐
                      ├──► S3: s3://photonestudios-photos/
iPhone (iPhoto/      ──┘
 Photos.app)/
```

- **Dropbox:** Liest direkt aus `~/Dropbox/` (oder deinem Dropbox-Pfad)
- **iPhone/Photos:** Exportiert aus der macOS Photos-App
- **S3:** Lädt alles in den `photonestudios-photos` Bucket hoch
- **Duplikate:** Werden automatisch erkannt (kein doppeltes Hochladen)
- **Struktur:** `s3://photonestudios-photos/YYYY/MM/dateiname.jpg`

## Setup (einmalig)

```bash
# 1. Dieses Repo klonen
git clone https://github.com/PhotonEsTuDios/staubsauger
cd staubsauger

# 2. Python-Abhängigkeiten installieren
pip install boto3 pillow

# 3. AWS-Profil prüfen
aws s3 ls --profile photon

# 4. Staubsauger starten
python staubsauger.py --profile photon --dropbox ~/Dropbox --iphoto
```

## Optionen

| Flag | Bedeutung |
|---|---|
| `--dropbox PATH` | Pfad zu deinem Dropbox-Ordner |
| `--iphoto` | Fotos aus macOS Photos.app exportieren |
| `--dry-run` | Nur anzeigen, was hochgeladen würde |
| `--since DATUM` | Nur Fotos ab diesem Datum (z.B. `2026-01-01`) |
| `--profile NAME` | AWS-Profil (Standard: `photon`) |

## Automatisch ausführen (täglich)

```bash
# In Terminal eingeben (läuft jeden Tag um 02:00 Uhr):
crontab -e
# Zeile hinzufügen:
0 2 * * * cd ~/staubsauger && python staubsauger.py --profile photon --dropbox ~/Dropbox --iphoto
```

---
*PhotonEsTuDios · Granada, Andalucía*
