"""iCloud-Asset -> flaches Metadaten-Dict (rein, ohne Netz; testbar).

Wichtig fürs Matching: das Archiv speichert `taken_at` als LOKALE Zeit ohne
Zeitzone (wie EXIF DateTimeOriginal / Dropbox-Dateiname). iCloud liefert
assetDate in UTC + timeZoneOffset (Sekunden) → wir rechnen auf Lokalzeit um.
"""
import base64
import datetime as dt
import os
import plistlib
from decimal import Decimal

from pyicloud.services.photos_cloudkit.mappers import record_field_value


def _dec(v):
    try:
        return Decimal(str(round(float(v), 7)))
    except (TypeError, ValueError):
        return None


def decode_location(raw):
    """locationEnc = (base64-)binäre plist mit lat/lon. Liefert (lat, lon) oder None."""
    if not raw:
        return None
    try:
        blob = raw if isinstance(raw, (bytes, bytearray)) else base64.b64decode(raw)
        d = plistlib.loads(bytes(blob))
    except Exception:
        return None
    if not isinstance(d, dict):
        return None
    lat = d.get("lat", d.get("latitude"))
    lon = d.get("lon", d.get("longitude"))
    if lat is None or lon is None:
        return None
    lat, lon = float(lat), float(lon)
    if (lat == 0 and lon == 0) or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return lat, lon


def local_time(utc, offset_s):
    if utc is None or utc.timestamp() <= 0:
        return None
    local = utc.astimezone(dt.timezone.utc).replace(tzinfo=None) + dt.timedelta(seconds=int(offset_s or 0))
    return local.strftime("%Y-%m-%dT%H:%M:%S")


def asset_meta(a):
    """PhotoAsset -> dict (nur JSON-taugliche Werte außer Decimal für GPS)."""
    rec = a.asset_record
    utc = a.asset_date
    tz = record_field_value(rec, "timeZoneOffset") or 0
    w, h = a.dimensions
    loc = decode_location(record_field_value(rec, "locationEnc"))
    if loc is None:
        lat, lon = record_field_value(rec, "locationLatitude"), record_field_value(rec, "locationLongitude")
        loc = (float(lat), float(lon)) if lat is not None and lon is not None else None
    name = a.filename or f"{a.id}"
    return {
        "icloud_id": a.id,
        "filename": name,
        "ext": os.path.splitext(name)[1].lower(),
        "bytes": a.size,
        "width": w,
        "height": h,
        "taken_at": local_time(utc, tz),
        "taken_at_utc": utc.strftime("%Y-%m-%dT%H:%M:%SZ") if utc and utc.timestamp() > 0 else None,
        "tz_offset": int(tz),
        "gps": [str(_dec(loc[0])), str(_dec(loc[1]))] if loc else None,
        "favorite": bool(record_field_value(rec, "isFavorite") or 0),
        "kind": a.item_type,  # "image" | "movie"
        "url": a.download_url("original"),
    }
