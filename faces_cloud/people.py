"""Personen: Gruppieren + Benennen (staubsauger#14, gallery#7/#9). Arbeitet nur über die Store-Schnittstelle.

Grundregel: Wahrheit liegt am **Gesicht** (cluster, person, ignored). Fotos (`face_clusters`,
`persons`) und die Übersicht faces/clusters.json werden daraus abgeleitet.

  run_cluster          alle Gesichter neu gruppieren; Namen gehen per Mehrheit auf die ganze Gruppe
  name(cluster, person) Gruppe benennen (Zusammenführen = zweite Gruppe gleich benennen); person="" entnennt
  hide(cluster)         Gruppe ausblenden (Fremde, Publikum)
  reject(sha, face)     ein falsches Gesicht aus seiner Gruppe nehmen
  name_face(sha, face, person)  ein einzelnes Gesicht benennen (Lightbox)
  sample(cluster, n, offset)    Stichprobe zum Prüfen: Ausreißer zuerst + ein paar typische (gallery#10)
  confirm(cluster, person, wrong, shown)  Name + „nicht diese Person“; Ähnliche im Rest werden mit entfernt
"""
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import numpy as np

import cluster as clu
import detect

WRITERS = 64  # I/O-gebunden; größte Gruppe (4.800 Gesichter) muss unter dem 30-s-CloudFront-Limit bleiben
SAMPLES = 12
SAMPLE_N = 40       # so viele Gesichter zeigt der Prüfen-Dialog auf einmal
TYPICAL_SHARE = 0.3  # Anteil typischer Gesichter in der ersten Stichprobe (Rest: Ausreißer = eher falsch)


def _parallel(fn, args):
    with ThreadPoolExecutor(max_workers=WRITERS) as pool:
        return list(pool.map(lambda a: fn(*a), args))


def clean_name(person):
    p = " ".join(str(person or "").split())[:60]
    if any(c in p for c in "<>\"\\/,"):  # Komma trennt Personen in der Gallery-URL
        raise ValueError("Ungültiger Name")
    return p


def refresh_photo(st, sha):
    """face_clusters/persons eines Fotos aus seinen Gesichtern neu ableiten."""
    faces = [f for f in st.faces_of_photo(sha) if not f.get("ignored")]
    st.set_photo_people(sha, sorted({f["cluster"] for f in faces if f.get("cluster")}),
                        sorted({f["person"] for f in faces if f.get("person")}))


def _summary_entry(c, group, person):
    best = sorted(group, key=lambda it: -float(it["score"]) * int(it["px"]))[:SAMPLES]
    return {"cluster": c, "size": len(group), "photos": len({str(it["sha256"]) for it in group}),
            "person": person, "samples": [it["crop"] for it in best]}


# ---- Gruppieren ------------------------------------------------------------------
def run_cluster(st, threshold=clu.THRESHOLD):
    t0 = time.time()
    items = st.all_faces()
    if not items:
        st.write_summary({"generated_at": st.now(), "faces": 0, "clusters": []})
        return {"faces": 0, "clusters": 0}
    vecs = np.stack([detect.bytes_to_vec(it["vec"]) for it in items])
    labels = clu.cluster(vecs, threshold)
    named = clu.persons_for_clusters(labels, [it.get("person") for it in items])
    # „nicht diese Person“ (gallery#10): Gesicht bleibt aus der Gruppe dieses Namens draußen
    labels = [None if c and not it.get("person") and it.get("not_person") and named.get(c) == it["not_person"] else c
              for it, c in zip(items, labels)]

    writes = []
    old_photo, new_photo = defaultdict(lambda: (set(), set())), defaultdict(lambda: (set(), set()))
    for it, c in zip(items, labels):
        sha = str(it["sha256"])
        person = it.get("person") or (named.get(c) if c else None)  # eigener Name schlägt Gruppenname
        if it.get("cluster") != c or it.get("person") != person:
            writes.append((st.set_face, sha, int(it["face"]), c, person))
        for store_, cc, pp in ((old_photo, it.get("cluster"), it.get("person")), (new_photo, c, person)):
            if cc:
                store_[sha][0].add(cc)
            if pp:
                store_[sha][1].add(pp)
    photo_writes = [(sha, sorted(new_photo[sha][0]), sorted(new_photo[sha][1]))
                    for sha in set(old_photo) | set(new_photo)
                    if (new_photo[sha][0], new_photo[sha][1]) != (old_photo[sha][0], old_photo[sha][1])]
    _parallel(lambda f, sha, face, c, p: f(sha, face, cluster=c, person=p), writes)
    _parallel(st.set_photo_people, photo_writes)

    groups = defaultdict(list)
    for it, c in zip(items, labels):
        if c:
            groups[c].append(it)
    st.write_summary({"generated_at": st.now(), "faces": len(items),
                      "clusters": [_summary_entry(c, groups[c], named.get(c)) for c in sorted(groups)]})
    out = {"faces": len(items), "clusters": len(groups), "faces_changed": len(writes),
           "photos_changed": len(photo_writes), "seconds": round(time.time() - t0, 1)}
    print("[cluster]", out)
    return out


# ---- Benennen (aus der Gallery) ------------------------------------------------------
def _patch_summary(st, cluster, **change):
    s = st.read_summary()
    keep = []
    for c in s.get("clusters", []):
        if c["cluster"] == cluster:
            if change.get("drop"):
                continue
            c.update({k: v for k, v in change.items() if k != "drop"})
        keep.append(c)
    s["clusters"] = keep
    st.write_summary(s)


def name(st, cluster, person):
    person = clean_name(person)
    t0 = time.time()
    faces = [f for f in st.faces_of_cluster(cluster) if not f.get("ignored")]
    t1 = time.time()
    _parallel(lambda f: st.set_face(str(f["sha256"]), int(f["face"]), person=person or None), [(f,) for f in faces])
    t2 = time.time()
    shas = sorted({str(f["sha256"]) for f in faces})
    old = {f.get("person") for f in faces if f.get("person")}
    if person and not (old - {person}):
        # schneller Weg (häufigster Fall: unbenannte Gruppe benennen): Name nur anhängen, 1 Aufruf je Foto
        _parallel(lambda sha: st.add_photo_person(sha, cluster, person), [(s,) for s in shas])
    else:
        # Entnennen/Umbenennen: Fotos genau aus ihren Gesichtern neu ableiten
        _parallel(lambda sha: refresh_photo(st, sha), [(s,) for s in shas])
    t3 = time.time()
    _patch_summary(st, cluster, person=person or None)
    print(f"[name] {cluster} faces={len(faces)} query={t1-t0:.1f}s faces={t2-t1:.1f}s photos={t3-t2:.1f}s summary={time.time()-t3:.1f}s")
    return {"cluster": cluster, "person": person or None, "faces": len(faces), "photos": len(shas)}


def hide(st, cluster):
    faces = st.faces_of_cluster(cluster)
    _parallel(lambda f: st.set_face(str(f["sha256"]), int(f["face"]), ignored=True, cluster=None, person=None),
              [(f,) for f in faces])
    shas = sorted({str(f["sha256"]) for f in faces})
    _parallel(lambda sha: refresh_photo(st, sha), [(s,) for s in shas])
    _patch_summary(st, cluster, drop=True)
    return {"cluster": cluster, "hidden": len(faces), "photos": len(shas)}


def reject(st, sha, face):
    st.set_face(sha, int(face), ignored=True, cluster=None, person=None)
    refresh_photo(st, sha)
    return {"sha": sha, "face": int(face), "ignored": True}


def name_face(st, sha, face, person):
    person = clean_name(person)
    st.set_face(sha, int(face), person=person or None, ignored=None)
    refresh_photo(st, sha)
    return {"sha": sha, "face": int(face), "person": person or None}


# ---- Prüfen: Stichprobe + Bestätigen (gallery#10) ---------------------------------------------
def _key(f):
    return str(f["sha256"]), int(f["face"])


def _with_vectors(st, faces):
    """(Gesichter mit Vektor, Matrix normiert) in derselben Reihenfolge."""
    vecs = st.vectors([_key(f) for f in faces])
    keep = [f for f in faces if _key(f) in vecs]
    if not keep:
        return [], np.zeros((0, 128), np.float32)
    m = np.stack([detect.bytes_to_vec(vecs[_key(f)]) for f in keep])
    m /= np.linalg.norm(m, axis=1, keepdims=True) + 1e-9
    return keep, m


def sample_order(sims, n=SAMPLE_N, typical=TYPICAL_SHARE):
    """Reihenfolge zum Zeigen: erste Seite = Ausreißer (kleinste Ähnlichkeit zum Gruppenkern) + gleichmäßig
    verteilte typische; danach die restlichen Ausreißer zuerst."""
    asc = list(np.argsort(sims, kind="stable"))
    k_typ = int(round(n * typical)) if len(asc) > n else 0
    first = asc[:n - k_typ]
    rest = asc[n - k_typ:]
    if k_typ and rest:
        pick = sorted({int(round(i * (len(rest) - 1) / max(k_typ - 1, 1))) for i in range(k_typ)})
        first += [rest[i] for i in pick]
        rest = [r for i, r in enumerate(rest) if i not in set(pick)]
    return [int(i) for i in first + rest]


def sample(st, cluster, n=SAMPLE_N, offset=0):
    faces, m = _with_vectors(st, [f for f in st.faces_of_cluster(cluster) if not f.get("ignored")])
    if not faces:
        return {"cluster": cluster, "size": 0, "faces": [], "next": None}
    centre = m.mean(axis=0)
    sims = m @ (centre / (np.linalg.norm(centre) + 1e-9))
    order = sample_order(sims, n)
    page = order[offset:offset + n]
    nxt = offset + n if offset + n < len(order) else None
    return {"cluster": cluster, "size": len(faces), "next": nxt,
            "faces": [{"sha": str(faces[i]["sha256"]), "face": int(faces[i]["face"]),
                       "crop": faces[i].get("crop") or f"faces/{faces[i]['sha256']}_{int(faces[i]['face'])}.jpg",
                       "sim": round(float(sims[i]), 3)} for i in page]}


def spread_wrong(m, good_idx, bad_idx):
    """Nächster Nachbar: Gesicht gilt als falsch, wenn es einem falschen ähnlicher ist als jedem richtigen."""
    if not len(bad_idx) or not len(good_idx):
        return set()
    sb = (m @ m[bad_idx].T).max(axis=1)
    sg = (m @ m[good_idx].T).max(axis=1)
    return {int(i) for i in np.nonzero(sb > sg)[0]}


def confirm(st, cluster, person, wrong, shown):
    person = clean_name(person)
    if not person:
        raise ValueError("Name fehlt")
    t0 = time.time()
    faces = [f for f in st.faces_of_cluster(cluster) if not f.get("ignored")]
    vfaces, m = _with_vectors(st, faces)
    idx = {_key(f): i for i, f in enumerate(vfaces)}
    wrong_k = {(str(w["sha"]), int(w["face"])) for w in wrong}
    shown_k = {(str(w["sha"]), int(w["face"])) for w in shown} | wrong_k
    bad = [idx[k] for k in wrong_k if k in idx]
    good = [idx[k] for k in shown_k - wrong_k if k in idx]
    auto = {_key(vfaces[i]) for i in spread_wrong(m, good, bad)} - shown_k
    out_k = wrong_k | auto
    out = [f for f in faces if _key(f) in out_k]
    keep = [f for f in faces if _key(f) not in out_k]
    old = {f.get("person") for f in faces if f.get("person")}

    _parallel(lambda f: st.set_face(*_key(f), cluster=None, person=None, not_person=person), [(f,) for f in out])
    _parallel(lambda f: st.set_face(*_key(f), person=person, not_person=None),
              [(f,) for f in keep if f.get("person") != person or f.get("not_person")])
    out_shas = {str(f["sha256"]) for f in out}
    keep_shas = {str(f["sha256"]) for f in keep}
    if old - {person}:
        refresh = out_shas | keep_shas
    else:
        refresh = out_shas
        _parallel(lambda sha: st.add_photo_person(sha, cluster, person), [(s,) for s in sorted(keep_shas - out_shas)])
    _parallel(lambda sha: refresh_photo(st, sha), [(s,) for s in sorted(refresh)])
    keep_crops = {f.get("crop") for f in keep}
    s = st.read_summary()
    for c in s.get("clusters", []):
        if c["cluster"] == cluster:
            c.update(person=person, size=len(keep), photos=len(keep_shas),
                     samples=[x for x in c.get("samples", []) if x in keep_crops] or c.get("samples", []))
    st.write_summary(s)
    res = {"cluster": cluster, "person": person, "faces": len(keep), "photos": len(keep_shas),
           "removed": len(wrong_k & set(map(_key, faces))), "auto_removed": len(auto)}
    print(f"[confirm] {res} {time.time() - t0:.1f}s")
    return res
