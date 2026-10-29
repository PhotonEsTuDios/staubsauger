"""Metadaten ohne exiftool (Lambda-tauglich, pure Python).

Bilder: exifread (JPEG/HEIC/TIFF/NEF/CR2/DNG …).
Videos (MOV/MP4/M4V): eigener Atom-Parser über eine read(offset, n)-Funktion —
funktioniert lokal auf Dateien UND per HTTP-Range auf presigned URLs, ohne das
ganze Video zu laden. Liest mvhd (Erstellzeit, UTC), tkhd (Breite/Höhe) und
Apple-Metadaten (com.apple.quicktime.creationdate / location.ISO6709 / make / model).
"""
import datetime as dt
import re
import struct
from decimal import Decimal

import exifread

IMG_EXTS = {".jpg", ".jpeg", ".heic", ".heif", ".tif", ".tiff", ".nef", ".cr2", ".cr3",
            ".arw", ".dng", ".raf", ".orf", ".rw2", ".png", ".webp"}
VID_EXTS = {".mov", ".mp4", ".m4v", ".3gp"}


def _dec(v):
    try:
        return Decimal(str(round(float(v), 7)))
    except Exception:
        return None


def _int(v):
    try:
        return Decimal(int(float(str(v))))
    except Exception:
        return None


def _ratio(v):
    try:
        return float(v.num) / float(v.den) if v.den else None
    except AttributeError:
        try:
            return float(v)
        except Exception:
            return None


def _iso(s):
    s = (s or "").strip().replace("\x00", "")
    m = re.match(r"(\d{4})[:\-](\d{2})[:\-](\d{2})[ T](\d{2}):(\d{2}):(\d{2})", s)
    if not m or m.group(1) in ("0000", "1904", "1970"):
        return None
    return "{}-{}-{}T{}:{}:{}".format(*m.groups())


def _gps(tags, coord, ref):
    v = tags.get(f"GPS {coord}")
    r = str(tags.get(f"GPS {ref}", "")).strip()
    if not v or not getattr(v, "values", None) or len(v.values) < 3:
        return None
    d, m, s = (_ratio(x) for x in v.values[:3])
    if d is None:
        return None
    val = d + (m or 0) / 60 + (s or 0) / 3600
    if r in ("S", "W"):
        val = -val
    return val if abs(val) > 1e-9 else None


def image_meta(fileobj):
    try:
        tags = exifread.process_file(fileobj, details=False)
    except Exception:
        return {}
    g = lambda k: str(tags[k]).strip() if k in tags else None
    out = {
        "taken_at": _iso(g("EXIF DateTimeOriginal") or g("EXIF DateTimeDigitized") or g("Image DateTime")),
        "camera_make": g("Image Make"),
        "camera_model": g("Image Model"),
        "lens": g("EXIF LensModel"),
        "iso": _int(tags["EXIF ISOSpeedRatings"].values[0]) if "EXIF ISOSpeedRatings" in tags and tags["EXIF ISOSpeedRatings"].values else None,
        "f_number": _dec(_ratio(tags["EXIF FNumber"].values[0])) if "EXIF FNumber" in tags else None,
        "exposure": g("EXIF ExposureTime"),
        "focal_length": _dec(_ratio(tags["EXIF FocalLength"].values[0])) if "EXIF FocalLength" in tags else None,
        "width": _int(g("EXIF ExifImageWidth")),
        "height": _int(g("EXIF ExifImageLength")),
    }
    lat, lon = _gps(tags, "GPSLatitude", "GPSLatitudeRef"), _gps(tags, "GPSLongitude", "GPSLongitudeRef")
    if lat is not None and lon is not None:
        out["gps_lat"], out["gps_lon"] = _dec(lat), _dec(lon)
    return {k: v for k, v in out.items() if v not in (None, "", "None")}


# ---- Video ---------------------------------------------------------------
_MAC_EPOCH = dt.datetime(1904, 1, 1)
_ISO6709 = re.compile(rb"([+-]\d{1,2}\.\d{3,})([+-]\d{1,3}\.\d{3,})")
_CDATE = re.compile(rb"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})[+-]\d{2}:?\d{2}")


def _find_moov(read, size):
    off = 0
    for _ in range(10000):
        if off + 8 > size:
            return None
        h = read(off, 16)
        if len(h) < 8:
            return None
        sz, typ = struct.unpack(">I4s", h[:8])
        hdr = 8
        if sz == 1:
            sz = struct.unpack(">Q", h[8:16])[0]; hdr = 16
        elif sz == 0:
            sz = size - off
        if sz < 8:
            return None
        if typ == b"moov":
            return off + hdr, sz - hdr
        off += sz
    return None


def video_meta(read, size):
    """read(offset, n) -> bytes"""
    loc = _find_moov(read, size)
    if not loc:
        return {}
    start, length = loc
    moov = read(start, min(length, 64 * 1024 * 1024))
    out = {}
    i = moov.find(b"mvhd")
    if i >= 4:
        ver = moov[i + 4]
        secs = struct.unpack(">Q", moov[i + 8:i + 16])[0] if ver == 1 else struct.unpack(">I", moov[i + 8:i + 12])[0]
        if secs > 0:
            try:
                t = _MAC_EPOCH + dt.timedelta(seconds=secs)
                if 1990 < t.year < 2100:
                    out["taken_at"] = t.replace(microsecond=0).isoformat()
            except OverflowError:
                pass
    m = _CDATE.search(moov)  # Apple: lokale Aufnahmezeit, besser als mvhd (UTC)
    if m:
        out["taken_at"] = m.group(1).decode()
    m = _ISO6709.search(moov)
    if m and (b"ISO6709" in moov or b"\xa9xyz" in moov):
        lat, lon = float(m.group(1)), float(m.group(2))
        if abs(lat) <= 90 and abs(lon) <= 180 and (lat or lon):
            out["gps_lat"], out["gps_lon"] = _dec(lat), _dec(lon)
    for tk in re.finditer(b"tkhd", moov):
        j = tk.start(); ver = moov[j + 4]
        base = j + 4 + (88 if ver == 1 else 76)
        if base + 8 <= len(moov):
            w, h = struct.unpack(">II", moov[base:base + 8])
            if w >> 16 and h >> 16:
                out["width"], out["height"] = Decimal(w >> 16), Decimal(h >> 16)
                break
    if b"com.apple.quicktime" in moov or b"\xa9mak" in moov:
        mm = re.search(rb"(iPhone[ \w,]{0,20}|iPad[ \w,]{0,20})", moov)
        if mm:
            out["camera_make"] = "Apple"
            out["camera_model"] = mm.group(1).decode("ascii", "ignore").strip()
    return out


def file_reader(path):
    f = open(path, "rb")
    def read(off, n):
        f.seek(off); return f.read(n)
    return read, f


def meta_for_file(path, ext, size):
    ext = ext.lower()
    if ext in IMG_EXTS:
        with open(path, "rb") as f:
            return image_meta(f)
    if ext in VID_EXTS:
        read, f = file_reader(path)
        try:
            return video_meta(read, size)
        finally:
            f.close()
    return {}
