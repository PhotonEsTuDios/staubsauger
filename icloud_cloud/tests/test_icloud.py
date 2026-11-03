"""Tests iCloud-Adapter (staubsauger#2) — ohne Netz, ohne AWS."""
import base64
import datetime as dt
import os
import plistlib
import sys
from decimal import Decimal

import pytest

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [HERE, os.path.join(HERE, "..", "s3importer")]

import assets  # noqa: E402
import handler  # noqa: E402
import session_store  # noqa: E402
from matching import ArchiveIndex  # noqa: E402


# ---- assets ----
def test_decode_location_base64_plist():
    raw = base64.b64encode(plistlib.dumps({"lat": 36.89, "lon": -3.42, "alt": 450.0}, fmt=plistlib.FMT_BINARY))
    assert assets.decode_location(raw.decode()) == (36.89, -3.42)


def test_decode_location_rejects_garbage_and_null_island():
    assert assets.decode_location("nicht-base64!!") is None
    assert assets.decode_location(None) is None
    assert assets.decode_location(plistlib.dumps({"lat": 0.0, "lon": 0.0})) is None


def test_local_time_uses_offset():
    utc = dt.datetime(2024, 5, 1, 10, 0, 0, tzinfo=dt.timezone.utc)
    assert assets.local_time(utc, 7200) == "2024-05-01T12:00:00"
    assert assets.local_time(dt.datetime.fromtimestamp(0, dt.timezone.utc), 0) is None


class FakeAsset:
    def __init__(self, fields, **kw):
        self.asset_record = {"fields": {k: {"value": v} for k, v in fields.items()}}
        self.id = kw.get("id", "A1")
        self.filename = kw.get("filename", "IMG_0001.HEIC")
        self.size = kw.get("size", 2_000_000)
        self.dimensions = kw.get("dims", (4032, 3024))
        self.asset_date = kw.get("utc", dt.datetime(2024, 5, 1, 10, 0, 0, tzinfo=dt.timezone.utc))
        self.item_type = "image"

    def download_url(self, version):
        return "https://cvws.icloud-content.com/x"


def test_asset_meta_full():
    loc = base64.b64encode(plistlib.dumps({"lat": 37.1, "lon": -3.6}, fmt=plistlib.FMT_BINARY)).decode()
    m = assets.asset_meta(FakeAsset({"timeZoneOffset": 3600, "locationEnc": loc, "isFavorite": 1}))
    assert m["taken_at"] == "2024-05-01T11:00:00"
    assert m["taken_at_utc"] == "2024-05-01T10:00:00Z"
    assert m["gps"] == ["37.1", "-3.6"]
    assert m["favorite"] is True and m["ext"] == ".heic"


# ---- matching ----
def _idx(*items):
    idx = ArchiveIndex()
    for it in items:
        idx.add(it)
    return idx


def test_match_name_bytes():
    idx = _idx({"sha256": "s1", "s3_key": "s1.heic", "original_filename": "IMG_0001.HEIC", "bytes": Decimal(5)})
    assert idx.match({"filename": "img_0001.heic", "bytes": 5}) == ("s1", "name+bytes")


def test_match_time_dims_rotated_dropbox_copy():
    idx = _idx({"sha256": "s2", "s3_key": "s2.jpg", "original_filename": "2024-05-01 11.00.00.jpg",
                "bytes": 9, "taken_at": "2024-05-01T11:00:00", "width": 3024, "height": 4032})
    m = {"filename": "IMG_1.HEIC", "bytes": 1, "taken_at": "2024-05-01T11:00:00", "width": 4032, "height": 3024}
    assert idx.match(m) == ("s2", "zeit+maße")


def test_match_time_only_when_unique_and_dims_not_contradicting():
    idx = _idx({"sha256": "s3", "s3_key": "k", "taken_at": "2024-05-01T11:00:00"})
    assert idx.match({"taken_at": "2024-05-01T11:00:00", "width": 10, "height": 20}) == ("s3", "zeit")
    idx = _idx({"sha256": "s4", "s3_key": "k", "taken_at": "2024-05-01T11:00:00", "width": 1, "height": 1})
    assert idx.match({"taken_at": "2024-05-01T11:00:00", "width": 10, "height": 20}) == (None, None)


def test_burst_same_second_is_ambiguous():
    idx = _idx({"sha256": "a", "s3_key": "a", "taken_at": "2024-05-01T11:00:00"},
               {"sha256": "b", "s3_key": "b", "taken_at": "2024-05-01T11:00:00"})
    assert idx.match({"taken_at": "2024-05-01T11:00:00"}) == (None, None)


def test_index_skips_internal_records():
    idx = _idx({"sha256": "__icloud__", "taken_at": "2024-05-01T11:00:00"},
               {"sha256": "__writetest__", "s3_key": "x"})
    assert not idx.by_time


# ---- classify ----
def test_classify_counts_and_gps_gain():
    idx = _idx({"sha256": "s1", "s3_key": "k", "taken_at": "2024-05-01T11:00:00"},
               {"sha256": "s9", "s3_key": "k", "icloud_id": "KNOWN"})
    metas = [{"icloud_id": "KNOWN"},
             {"icloud_id": "B", "taken_at": "2024-05-01T11:00:00", "gps": ["1", "2"]},
             {"icloud_id": "C", "taken_at": "2030-01-01T00:00:00"}]
    enrich, imports, c = handler.classify(metas, idx)
    assert (c["known"], c["matched"], c["new"], c["gps_gain"]) == (1, 1, 1, 1)
    assert enrich[0]["sha256"] == "s1" and imports[0]["icloud_id"] == "C"


# ---- enrich ----
class FakeTable:
    def __init__(self):
        self.calls = []

    def update_item(self, **kw):
        self.calls.append(kw)


def test_enrich_never_overwrites_existing_metadata():
    t = FakeTable()
    handler.enrich_one({"sha256": "s1", "icloud_id": "B", "taken_at": "2024-05-01T11:00:00",
                        "gps": ["37.1", "-3.6"], "favorite": True, "match": "zeit"}, t)
    kw = t.calls[0]
    assert "#gps_lat=if_not_exists(#gps_lat, :gps_lat)" in kw["UpdateExpression"]
    assert "#taken_at=if_not_exists(#taken_at, :taken_at)" in kw["UpdateExpression"]
    assert kw["ExpressionAttributeValues"][":gps_lat"] == Decimal("37.1")
    assert kw["ConditionExpression"] == "attribute_exists(sha256)"


# ---- session ----
def test_session_pack_roundtrip(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "dani.cookiejar").write_text("C")
    (tmp_path / "a" / "dani.session").write_text("{}")
    files = session_store.pack_dir(tmp_path / "a")
    session_store.unpack_dir(files, tmp_path / "b")
    assert session_store.pack_dir(tmp_path / "b") == files


def test_session_unpack_rejects_traversal(tmp_path):
    with pytest.raises(ValueError):
        session_store.unpack_dir({"../evil": "x"}, tmp_path)


def test_open_service_requires_trusted_session(tmp_path):
    class Svc:
        ok = False

        def __init__(self, *a, **kw):
            assert kw["password"] is None and kw["authenticate"] is False

        def _try_reuse_cached_session(self, force, pause):
            return Svc.ok

        def _update_state(self):
            pass

    with pytest.raises(session_store.LoginNeeded):
        session_store.open_service("x", str(tmp_path), Svc)
    Svc.ok = True
    assert isinstance(session_store.open_service("x", str(tmp_path), Svc), Svc)


def test_cap_page_haelt_limit_exakt():
    from handler import cap_page
    page = list(range(100))
    assert len(cap_page(page, 0, 50)) == 50
    assert len(cap_page(page, 0, 0)) == 100      # kein Limit
    assert cap_page(page, 100, 50) == []          # schon erreicht
    assert len(cap_page(page, 100, 130)) == 30
