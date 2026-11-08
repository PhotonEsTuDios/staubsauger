"""Tests Gesichtserkennung (staubsauger#14):  ~/PhotonEsTuDios/.venv/bin/python -m pytest faces_cloud/tests -q

Braucht die Modelle in faces_cloud/models (./build.sh lädt sie mit Prüfsumme).
Testbilder: OpenCV-Samples (BSD), ohne AWS (Fake-Speicher).
"""
import json
import os
import sys

import cv2
import numpy as np
import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.dirname(HERE))
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")

import cluster as clu  # noqa: E402
import detect  # noqa: E402


def _img(name):
    with open(os.path.join(HERE, "data", name), "rb") as f:
        return f.read()


# ---- detect ------------------------------------------------------------------------
@pytest.fixture(scope="module")
def messi():
    return detect.find_faces(detect.load_image(_img("messi5.jpg")))


@pytest.fixture(scope="module")
def lena():
    return detect.find_faces(detect.load_image(_img("lena.jpg")))


def test_findet_genau_ein_gesicht(messi, lena):
    assert len(messi) == 1 and len(lena) == 1


def test_vektor_format(messi):
    v = messi[0]["vec"]
    assert v.shape == (128,) and v.dtype == np.float32
    assert abs(np.linalg.norm(v) - 1) < 1e-4
    assert all(0 <= x <= 1 for x in messi[0]["box"])


def test_ausschnitt_ist_jpeg(messi):
    crop = messi[0]["crop"]
    assert crop[:2] == b"\xff\xd8"
    assert cv2.imdecode(np.frombuffer(crop, np.uint8), 1).shape == (detect.CROP_PX, detect.CROP_PX, 3)


def test_gleiche_person_aehnlich_andere_nicht(messi, lena):
    flipped = detect.find_faces(cv2.flip(detect.load_image(_img("messi5.jpg")), 1))
    assert detect.similarity(messi[0]["vec"], flipped[0]["vec"]) > clu.THRESHOLD
    assert detect.similarity(messi[0]["vec"], lena[0]["vec"]) < clu.THRESHOLD


def test_vektor_binaer_rundreise(messi):
    v = messi[0]["vec"]
    b = detect.vec_to_bytes(v)
    assert len(b) == 256
    assert detect.similarity(v, detect.bytes_to_vec(b)) > 0.999


def test_grosses_bild_wird_verkleinert():
    big = cv2.resize(detect.load_image(_img("lena.jpg")), (4000, 4000))
    ok, buf = cv2.imencode(".jpg", big)
    im = detect.load_image(buf.tobytes())
    assert max(im.shape[:2]) <= detect.MAXDIM
    assert len(detect.find_faces(im)) == 1


def test_bild_ohne_gesicht():
    assert detect.find_faces(np.full((400, 600, 3), 128, np.uint8)) == []


# ---- cluster -------------------------------------------------------------------------
def _synthetic(n_people=4, per=15, noise=0.25, seed=1):
    rng = np.random.default_rng(seed)
    centers = rng.normal(size=(n_people, 128))
    vecs, truth = [], []
    for p, c in enumerate(centers):
        for _ in range(per):
            v = c / np.linalg.norm(c) + rng.normal(scale=noise / np.sqrt(128), size=128)
            vecs.append(v / np.linalg.norm(v))
            truth.append(p)
    return np.array(vecs, np.float32), truth


def test_cluster_trennt_personen_sauber():
    vecs, truth = _synthetic()
    labels = clu.cluster(vecs)
    assert len(set(labels)) == 4 and None not in labels
    for p in range(4):  # jede Person genau eine Gruppe, keine Gruppe mit zwei Personen
        assert len({l for l, t in zip(labels, truth) if t == p}) == 1
    assert len({(l, t) for l, t in zip(labels, truth)}) == 4


def test_cluster_einzelgesichter_ohne_gruppe():
    vecs, _ = _synthetic(n_people=2, per=10)
    lonely = np.random.default_rng(9).normal(size=(1, 128)).astype(np.float32)
    lonely /= np.linalg.norm(lonely)
    labels = clu.cluster(np.vstack([vecs, lonely]))
    assert labels[-1] is None


def test_cluster_ids_nach_groesse():
    a, _ = _synthetic(n_people=1, per=20, seed=2)
    b, _ = _synthetic(n_people=1, per=5, seed=3)
    labels = clu.cluster(np.vstack([b, a]))
    assert labels[-1] == "c0001" and labels[0] == "c0002"


def test_cluster_leer():
    assert clu.cluster(np.zeros((0, 128), np.float32)) == []


def test_namen_per_mehrheit():
    clusters = ["c0001", "c0001", "c0001", "c0002", None]
    persons = ["Ana", "Ana", "Bea", None, "Zoe"]
    assert clu.persons_for_clusters(clusters, persons) == {"c0001": "Ana"}


# ---- handler mit Fake-Speicher (Ablauf, Idempotenz, Fehler) ------------------------------
class FakeStore:
    def __init__(self, files):
        self.files, self.done, self.saved, self.errors, self.progress = files, set(), {}, {}, []

    def already_done(self, sha):
        return sha in self.done

    def read_original(self, key):
        return self.files[key]

    def save_faces(self, sha, faces, vec_to_bytes):
        self.saved[sha] = [vec_to_bytes(f["vec"]) for f in faces]
        self.done.add(sha)

    def mark_error(self, sha, err):
        self.errors[sha] = err

    def add_progress(self, done, faces, err):
        self.progress.append((done, faces, err))


@pytest.fixture
def fake(monkeypatch):
    import handler
    fs = FakeStore({"a.jpg": _img("messi5.jpg"), "b.jpg": _img("lena.jpg"), "kaputt.jpg": b"nope"})
    monkeypatch.setattr(handler, "_store", fs)
    return handler, fs


def test_worker_paket(fake):
    handler, fs = fake
    tasks = [{"sha": "a", "key": "a.jpg"}, {"sha": "b", "key": "b.jpg"}, {"sha": "k", "key": "kaputt.jpg"}]
    out = handler.handler({"Records": [{"messageId": "m1", "body": json.dumps(tasks)}]}, None)
    assert out == {"batchItemFailures": []}
    assert set(fs.saved) == {"a", "b"} and "k" in fs.errors
    assert fs.progress == [(2, 2, 1)]


def test_worker_idempotent(fake):
    handler, fs = fake
    fs.done.add("a")
    assert handler.process({"sha": "a", "key": "a.jpg"}) == ("skip", 0)
    assert "a" not in fs.saved


class MemStore:
    """Speicher im RAM mit derselben Schnittstelle wie store.Store (Gesichter + Fotos + Übersicht)."""

    def __init__(self, faces):
        self.faces = {(f["sha256"], f["face"]): dict(f) for f in faces}
        self.photos, self.summary, self.writes = {}, {"clusters": []}, 0

    def now(self):
        return "2026-11-03T00:00:00Z"

    def all_faces(self):
        return [dict(f) for f in self.faces.values() if not f.get("ignored")]

    def faces_of_cluster(self, c):
        return [dict(f) for f in self.faces.values() if f.get("cluster") == c]

    def faces_of_photo(self, sha):
        return [dict(f) for (s, _), f in self.faces.items() if s == sha]

    def vectors(self, keys):
        return {(s, int(f)): self.faces[(s, int(f))]["vec"] for s, f in keys if (s, int(f)) in self.faces}

    def set_face(self, sha, face, **fields):
        self.writes += 1
        f = self.faces[(sha, face)]
        for k, v in fields.items():
            if v is None:
                f.pop(k, None)
            else:
                f[k] = v

    def set_photo_people(self, sha, clusters, persons):
        self.photos[sha] = (list(clusters), list(persons))

    def add_photo_person(self, sha, cluster, person):
        cl, ps = self.photos.get(sha, ([], []))
        self.photos[sha] = (sorted(set(cl) | {cluster}), sorted(set(ps) | {person}))

    def read_summary(self):
        return json.loads(json.dumps(self.summary))

    def write_summary(self, s):
        self.summary = s


def _people_store(seed=1):
    """2 Personen à 6 Fotos (je 1 Gesicht), p0 schon als Ana benannt."""
    vecs, _ = _synthetic(n_people=2, per=6, seed=seed)
    return MemStore([{"sha256": f"p{i}", "face": 0, "vec": detect.vec_to_bytes(v), "px": 80, "score": 0.95,
                      "crop": f"faces/p{i}_0.jpg", **({"person": "Ana"} if i == 0 else {})}
                     for i, v in enumerate(vecs)])


@pytest.fixture
def mem(monkeypatch):
    import handler
    st = _people_store()
    monkeypatch.setattr(handler, "_store", st)
    return handler, st


def test_cluster_ablauf_name_geht_auf_gruppe(mem):
    handler, st = mem
    out = handler.handler({"mode": "cluster"}, None)
    assert out["clusters"] == 2 and out["photos_changed"] == 12
    ana = st.faces[("p0", 0)]["cluster"]
    assert all(st.photos[f"p{i}"] == ([ana], ["Ana"]) for i in range(6))
    assert all(st.faces[(f"p{i}", 0)]["person"] == "Ana" for i in range(6))
    assert all(st.photos[f"p{i}"][1] == [] for i in range(6, 12))
    assert sorted(c["size"] for c in st.summary["clusters"]) == [6, 6]
    assert {c["person"] for c in st.summary["clusters"]} == {"Ana", None}


def test_cluster_zweiter_lauf_schreibt_nichts(mem):
    handler, st = mem
    handler.handler({"mode": "cluster"}, None)
    st.writes = 0
    out = handler.handler({"mode": "cluster"}, None)
    assert out["faces_changed"] == 0 and out["photos_changed"] == 0 and st.writes == 0


def test_benennen_und_zusammenfuehren(mem):
    handler, st = mem
    handler.handler({"mode": "cluster"}, None)
    other = st.faces[("p6", 0)]["cluster"]
    out = handler.handler({"mode": "name", "cluster": other, "person": "  Santi  "}, None)
    assert out == {"cluster": other, "person": "Santi", "faces": 6, "photos": 6}
    assert st.photos["p7"] == ([other], ["Santi"])
    assert next(c for c in st.summary["clusters"] if c["cluster"] == other)["person"] == "Santi"
    # Zusammenführen = zweite Gruppe gleich benennen -> nach dem nächsten Lauf bleibt der Name
    handler.handler({"mode": "cluster"}, None)
    assert st.photos["p7"][1] == ["Santi"]


def test_benennen_zwei_gesichter_auf_einem_foto(mem):
    handler, st = mem
    handler.handler({"mode": "cluster"}, None)
    ana, other = st.faces[("p0", 0)]["cluster"], st.faces[("p6", 0)]["cluster"]
    # p6 bekommt zusätzlich ein Ana-Gesicht -> nach Benennen der anderen Gruppe beide Namen am Foto
    st.faces[("p6", 1)] = {**st.faces[("p0", 0)], "sha256": "p6", "face": 1}
    st.photos["p6"] = ([ana, other], ["Ana"])
    handler.handler({"mode": "name", "cluster": other, "person": "Santi"}, None)
    assert st.photos["p6"] == (sorted([ana, other]), ["Ana", "Santi"])


def test_umbenennen_leitet_neu_ab(mem):
    handler, st = mem
    handler.handler({"mode": "cluster"}, None)
    ana = st.faces[("p0", 0)]["cluster"]
    handler.handler({"mode": "name", "cluster": ana, "person": "Anna"}, None)
    assert st.photos["p1"] == ([ana], ["Anna"])  # alter Name weg, nicht nur angehängt


def test_entnennen(mem):
    handler, st = mem
    handler.handler({"mode": "cluster"}, None)
    ana = st.faces[("p0", 0)]["cluster"]
    handler.handler({"mode": "name", "cluster": ana, "person": ""}, None)
    assert all("person" not in st.faces[(f"p{i}", 0)] for i in range(6))
    assert st.photos["p3"] == ([ana], [])


def test_ausblenden(mem):
    handler, st = mem
    handler.handler({"mode": "cluster"}, None)
    other = st.faces[("p6", 0)]["cluster"]
    handler.handler({"mode": "hide", "cluster": other}, None)
    assert all(st.faces[(f"p{i}", 0)].get("ignored") for i in range(6, 12))
    assert st.photos["p8"] == ([], [])
    assert other not in [c["cluster"] for c in st.summary["clusters"]]
    out = handler.handler({"mode": "cluster"}, None)  # bleibt ausgeblendet
    assert out["clusters"] == 1


def test_falsches_gesicht_raus(mem):
    handler, st = mem
    handler.handler({"mode": "cluster"}, None)
    handler.handler({"mode": "reject", "sha": "p2", "face": 0}, None)
    assert st.faces[("p2", 0)]["ignored"] is True and "person" not in st.faces[("p2", 0)]
    assert st.photos["p2"] == ([], [])


def test_einzelnes_gesicht_benennen(mem):
    handler, st = mem
    handler.handler({"mode": "cluster"}, None)
    handler.handler({"mode": "name_face", "sha": "p9", "face": 0, "person": "Nepo"}, None)
    assert st.photos["p9"][1] == ["Nepo"]
    handler.handler({"mode": "cluster"}, None)  # eigener Name schlägt den Gruppennamen
    assert st.faces[("p9", 0)]["person"] == "Nepo"


def test_name_ohne_sonderzeichen():
    import people
    with pytest.raises(ValueError):
        people.clean_name("<script>")
    with pytest.raises(ValueError):
        people.clean_name("Santi, Nepo")
    assert people.clean_name("  Santi   Häring ") == "Santi Häring"


# ---- Prüfen mit Stichprobe (gallery#10) ------------------------------------------------------
def _mixed_store(n_main=30, n_wrong=6, seed=3):
    """Eine Gruppe c0001: 30 Gesichter von A + 6 von B (falsch hineingeraten)."""
    vecs, labels = _synthetic(n_people=2, per=max(n_main, n_wrong), seed=seed)
    a = [v for v, l in zip(vecs, labels) if l == 0][:n_main]
    b = [v for v, l in zip(vecs, labels) if l == 1][:n_wrong]
    faces = [{"sha256": f"a{i}", "face": 0, "vec": detect.vec_to_bytes(v), "px": 80, "score": 0.95,
              "crop": f"faces/a{i}_0.jpg", "cluster": "c0001"} for i, v in enumerate(a)]
    faces += [{"sha256": f"b{i}", "face": 0, "vec": detect.vec_to_bytes(v), "px": 80, "score": 0.95,
               "crop": f"faces/b{i}_0.jpg", "cluster": "c0001"} for i, v in enumerate(b)]
    st = MemStore(faces)
    st.summary = {"clusters": [{"cluster": "c0001", "size": 36, "photos": 36, "person": None,
                                "samples": ["faces/a0_0.jpg", "faces/b0_0.jpg"]}]}
    return st


def test_stichprobe_ausreisser_zuerst(monkeypatch):
    import handler
    st = _mixed_store()
    monkeypatch.setattr(handler, "_store", st)
    out = handler.handler({"mode": "sample", "cluster": "c0001", "n": 10}, None)
    assert out["size"] == 36 and len(out["faces"]) == 10 and out["next"] == 10
    first = [f["sha"] for f in out["faces"][:6]]
    assert sum(s.startswith("b") for s in first) >= 5  # die Fremden stehen vorn
    rest = handler.handler({"mode": "sample", "cluster": "c0001", "n": 10, "offset": 30}, None)
    assert len(rest["faces"]) == 6 and rest["next"] is None


def test_sample_order_vollstaendig_ohne_doppelte():
    import people
    sims = np.random.default_rng(1).random(97)
    order = people.sample_order(sims, 40)
    assert sorted(order) == list(range(97))
    assert len(people.sample_order(np.array([0.5, 0.2]), 40)) == 2


def test_bestaetigen_entfernt_markierte_und_aehnliche(monkeypatch):
    import handler
    st = _mixed_store()
    monkeypatch.setattr(handler, "_store", st)
    # Dani sieht nur 10 (8 von A, 2 von B) und markiert die 2 falschen
    shown = [{"sha": f"a{i}", "face": 0} for i in range(8)] + [{"sha": "b0", "face": 0}, {"sha": "b1", "face": 0}]
    wrong = shown[-2:]
    out = handler.handler({"mode": "confirm", "cluster": "c0001", "person": "Santi", "wrong": wrong, "shown": shown}, None)
    assert out["removed"] == 2 and out["auto_removed"] == 4 and out["faces"] == 30
    assert all(st.faces[(f"a{i}", 0)]["person"] == "Santi" for i in range(30))
    for i in range(6):
        f = st.faces[(f"b{i}", 0)]
        assert "person" not in f and "cluster" not in f and f["not_person"] == "Santi"
        assert st.photos[f"b{i}"] == ([], [])
    assert st.photos["a5"] == (["c0001"], ["Santi"])
    c = st.summary["clusters"][0]
    assert c["person"] == "Santi" and c["size"] == 30 and c["samples"] == ["faces/a0_0.jpg"]


def test_bestaetigen_ohne_falsche_benennt_alle(monkeypatch):
    import handler
    st = _mixed_store(n_wrong=0)
    monkeypatch.setattr(handler, "_store", st)
    out = handler.handler({"mode": "confirm", "cluster": "c0001", "person": "Ana", "wrong": [],
                           "shown": [{"sha": "a1", "face": 0}]}, None)
    assert out["faces"] == 30 and out["removed"] == 0 and out["auto_removed"] == 0


def test_bestaetigen_braucht_namen(monkeypatch):
    import handler
    monkeypatch.setattr(handler, "_store", _mixed_store())
    with pytest.raises(ValueError):
        handler.handler({"mode": "confirm", "cluster": "c0001", "person": " ", "wrong": [], "shown": []}, None)


def test_nicht_person_bleibt_nach_neuem_lauf_draussen(monkeypatch):
    import handler
    st = _mixed_store()
    monkeypatch.setattr(handler, "_store", st)
    shown = [{"sha": "a0", "face": 0}, {"sha": "b0", "face": 0}]
    handler.handler({"mode": "confirm", "cluster": "c0001", "person": "Santi", "wrong": shown[1:], "shown": shown}, None)
    handler.handler({"mode": "cluster"}, None)  # nächtlicher Lauf
    assert all(st.faces[(f"a{i}", 0)]["person"] == "Santi" for i in range(30))
    assert all(st.faces[(f"b{i}", 0)].get("person") != "Santi" for i in range(6))
    assert st.faces[("b0", 0)].get("cluster") != st.faces[("a0", 0)]["cluster"]


# ---- „✕ nicht <Person>“ (gallery#16) ------------------------------------------------------
def _named_mixed(monkeypatch):
    import handler
    st = _mixed_store()
    monkeypatch.setattr(handler, "_store", st)
    for f in st.faces.values():
        f["person"] = "Santi"  # Gruppe komplett als Santi benannt, 6 Fremde (b*) stecken drin
    st.summary["clusters"][0]["person"] = "Santi"
    return handler, st


def test_nicht_person_nimmt_aehnliche_mit(monkeypatch):
    handler, st = _named_mixed(monkeypatch)
    out = handler.handler({"mode": "not_person", "sha": "b0", "face": 0, "person": "Santi"}, None)
    assert out["removed"] == 6 and out["auto_removed"] == 5
    for i in range(6):
        f = st.faces[(f"b{i}", 0)]
        assert "person" not in f and "cluster" not in f and f["not_person"] == "Santi"
    assert all(st.faces[(f"a{i}", 0)]["person"] == "Santi" for i in range(30))
    assert st.summary["clusters"][0]["size"] == 30
    assert st.photos["b3"] == ([], [])


def test_nicht_person_bleibt_draussen_nach_neuem_lauf(monkeypatch):
    handler, st = _named_mixed(monkeypatch)
    handler.handler({"mode": "not_person", "sha": "b0", "face": 0, "person": "Santi"}, None)
    handler.handler({"mode": "cluster"}, None)
    assert all(st.faces[(f"b{i}", 0)].get("person") != "Santi" for i in range(6))
    assert all(st.faces[(f"a{i}", 0)]["person"] == "Santi" for i in range(30))


def test_nicht_person_einzelnes_gesicht_ohne_aehnliche(monkeypatch):
    handler, st = _named_mixed(monkeypatch)
    out = handler.handler({"mode": "not_person", "sha": "a3", "face": 0, "person": "Santi"}, None)
    assert out["removed"] == 1  # echtes Santi-Gesicht: keine anderen Santis mitreißen
    assert sum(1 for i in range(30) if st.faces[(f"a{i}", 0)].get("person") == "Santi") == 29
