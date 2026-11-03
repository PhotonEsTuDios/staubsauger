"""Personen: Gruppieren + Benennen (staubsauger#14, gallery#7/#9). Arbeitet nur über die Store-Schnittstelle.

Grundregel: Wahrheit liegt am **Gesicht** (cluster, person, ignored). Fotos (`face_clusters`,
`persons`) und die Übersicht faces/clusters.json werden daraus abgeleitet.

  run_cluster          alle Gesichter neu gruppieren; Namen gehen per Mehrheit auf die ganze Gruppe
  name(cluster, person) Gruppe benennen (Zusammenführen = zweite Gruppe gleich benennen); person="" entnennt
  hide(cluster)         Gruppe ausblenden (Fremde, Publikum)
  reject(sha, face)     ein falsches Gesicht aus seiner Gruppe nehmen
  name_face(sha, face, person)  ein einzelnes Gesicht benennen (Lightbox)
"""
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import numpy as np

import cluster as clu
import detect

WRITERS = 64  # I/O-gebunden; größte Gruppe (4.800 Gesichter) muss unter dem 30-s-CloudFront-Limit bleiben
SAMPLES = 12


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
