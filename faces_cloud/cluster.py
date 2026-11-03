"""Gesichter zu Personen-Gruppen bündeln (staubsauger#14). Nur numpy, kein AWS.

Verfahren: k-nächste-Nachbarn-Graph (Kosinus >= THRESHOLD), dann Chinese Whispers.
Stabil über Läufe: Gruppen-IDs ändern sich, Namen hängen deshalb an einzelnen
Gesichtern (`person` am Face-Item) und werden per Mehrheit auf die Gruppe übertragen.
"""
import random
from collections import Counter, defaultdict

import numpy as np

THRESHOLD = 0.45   # strenger als die SFace-Empfehlung 0,363: lieber zwei Gruppen als eine falsche
K = 20             # Nachbarn je Gesicht
MIN_CLUSTER = 3    # kleinere Gruppen = Einzelgesichter, bekommen keine Gruppe
BLOCK = 2048


def knn_graph(vecs: np.ndarray, threshold=THRESHOLD, k=K):
    """Kanten (i, j, w) für die k ähnlichsten Nachbarn je Gesicht mit w >= threshold."""
    n = len(vecs)
    nbrs = [[] for _ in range(n)]
    k = min(k, n - 1)
    if k <= 0:
        return nbrs
    for s in range(0, n, BLOCK):
        sim = vecs[s:s + BLOCK] @ vecs.T
        for r in range(sim.shape[0]):
            sim[r, s + r] = -1.0  # sich selbst nicht
        idx = np.argpartition(-sim, k - 1, axis=1)[:, :k]
        for r in range(sim.shape[0]):
            i = s + r
            for j in idx[r]:
                w = float(sim[r, j])
                if w >= threshold:
                    nbrs[i].append((int(j), w))
    # symmetrisch machen
    sym = [dict() for _ in range(n)]
    for i, lst in enumerate(nbrs):
        for j, w in lst:
            sym[i][j] = w
            sym[j][i] = w
    return [list(d.items()) for d in sym]


def chinese_whispers(nbrs, iterations=20, seed=7):
    rnd = random.Random(seed)
    labels = list(range(len(nbrs)))
    order = list(range(len(nbrs)))
    for _ in range(iterations):
        rnd.shuffle(order)
        changed = 0
        for i in order:
            if not nbrs[i]:
                continue
            score = defaultdict(float)
            for j, w in nbrs[i]:
                score[labels[j]] += w
            best = max(score.items(), key=lambda kv: (kv[1], -kv[0]))[0]
            if best != labels[i]:
                labels[i] = best
                changed += 1
        if not changed:
            break
    return labels


def cluster(vecs: np.ndarray, threshold=THRESHOLD, min_cluster=MIN_CLUSTER) -> list:
    """Liefert je Gesicht eine Gruppen-ID ('c0001' = größte Gruppe) oder None."""
    if len(vecs) == 0:
        return []
    labels = chinese_whispers(knn_graph(np.asarray(vecs, dtype=np.float32), threshold))
    sizes = Counter(labels)
    ranked = [l for l, c in sorted(sizes.items(), key=lambda kv: (-kv[1], kv[0])) if c >= min_cluster]
    names = {l: f"c{n + 1:04d}" for n, l in enumerate(ranked)}
    return [names.get(l) for l in labels]


def persons_for_clusters(clusters: list, persons: list) -> dict:
    """Gruppe -> Name, per Mehrheit der schon benannten Gesichter in der Gruppe."""
    votes = defaultdict(Counter)
    for c, p in zip(clusters, persons):
        if c and p:
            votes[c][p] += 1
    return {c: v.most_common(1)[0][0] for c, v in votes.items()}
