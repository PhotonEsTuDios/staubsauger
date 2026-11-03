"""Abgleich iCloud-Asset <-> vorhandenes Archiv OHNE Download (staubsauger#2).

Das Archiv kennt die Datei-Inhalte (SHA-256), iCloud liefert keinen SHA-256.
Deshalb zuerst billig über Metadaten:
  1. gleicher Dateiname + gleiche Bytezahl          → sicher dieselbe Datei
  2. gleiche Lokalzeit (Sekunde) + gleiche Maße     → dieselbe Aufnahme (auch
     Dropbox-JPG-Kopie eines HEIC: Maße bleiben, Bytes/Namen nicht)
  3. gleiche Lokalzeit allein, nur wenn eindeutig    → wahrscheinlich (Maße fehlen
     bei vielen Dropbox-Einträgen)
Mehrdeutig (z. B. Serienbild in derselben Sekunde) → kein Treffer, dann lädt der
Worker das Original und dedupliziert über SHA-256.
"""
from collections import defaultdict


def _dims(w, h):
    try:
        a, b = int(w), int(h)
    except (TypeError, ValueError):
        return None
    return tuple(sorted((a, b))) if a and b else None


class ArchiveIndex:
    def __init__(self):
        self.by_name = defaultdict(set)
        self.by_time_dims = defaultdict(set)
        self.by_time = defaultdict(set)
        self.icloud_ids = set()
        self.has_gps = set()
        self.dims = {}

    def add(self, item):
        sha = item.get("sha256")
        if not sha or sha.startswith("__") or not item.get("s3_key"):
            return
        if item.get("icloud_id"):
            self.icloud_ids.add(item["icloud_id"])
        if item.get("gps_lat") is not None:
            self.has_gps.add(sha)
        name, size = item.get("original_filename"), item.get("bytes")
        if name and size:
            self.by_name[(name.lower(), int(size))].add(sha)
        t = (item.get("taken_at") or "")[:19]
        if len(t) == 19:
            self.by_time[t].add(sha)
            d = _dims(item.get("width"), item.get("height"))
            if d:
                self.dims[sha] = d
                self.by_time_dims[(t, d)].add(sha)

    def match(self, m):
        """-> (sha, wie) oder (None, None)."""
        if m.get("filename") and m.get("bytes"):
            hit = self.by_name.get((m["filename"].lower(), int(m["bytes"])))
            if hit and len(hit) == 1:
                return next(iter(hit)), "name+bytes"
        t = (m.get("taken_at") or "")[:19]
        if len(t) == 19:
            d = _dims(m.get("width"), m.get("height"))
            hit = self.by_time_dims.get((t, d)) if d else None
            if hit and len(hit) == 1:
                return next(iter(hit)), "zeit+maße"
            hit = self.by_time.get(t)
            if hit and len(hit) == 1:
                sha = next(iter(hit))
                known = self.dims.get(sha)
                if not (known and d and known != d):  # andere Maße = anderes Bild
                    return sha, "zeit"
        return None, None
