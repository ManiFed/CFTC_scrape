"""Embedding-based theme clustering with HDBSCAN."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

from cftc_pipeline.config import settings

logger = logging.getLogger(__name__)

EMBED_MODEL = "all-MiniLM-L6-v2"


@dataclass
class ClusterResult:
    submission_id: int
    cluster_id: int  # -1 = noise / unclustered
    relevance_score: float
    is_representative: bool


@dataclass
class ClusterSummary:
    cluster_id: int
    auto_label: str
    keywords: list[str]
    member_ids: list[int]
    representative_ids: list[int]
    total_count: int


def embed_texts(texts: list[str]) -> np.ndarray:
    """Generate sentence embeddings using sentence-transformers."""
    from sentence_transformers import SentenceTransformer

    logger.info("Loading embedding model: %s", EMBED_MODEL)
    model = SentenceTransformer(EMBED_MODEL)
    logger.info("Embedding %d texts", len(texts))
    embeddings = model.encode(texts, batch_size=settings.batch_size_embed, show_progress_bar=True)
    return embeddings


def cluster_embeddings(
    embeddings: np.ndarray,
    min_cluster_size: int = 5,
    min_samples: int = 3,
) -> np.ndarray:
    """Run HDBSCAN clustering, return label array."""
    import hdbscan

    logger.info("Running HDBSCAN on %d embeddings", len(embeddings))
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="euclidean",
        cluster_selection_method="eom",
    )
    labels = clusterer.fit_predict(embeddings)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = list(labels).count(-1)
    logger.info("Found %d clusters, %d noise points", n_clusters, n_noise)
    return labels


def extract_keywords(texts: list[str], top_n: int = 8) -> list[str]:
    """Extract representative keywords from a cluster's texts using TF-IDF."""
    from sklearn.feature_extraction.text import TfidfVectorizer

    if not texts:
        return []
    try:
        vec = TfidfVectorizer(max_features=200, stop_words="english", ngram_range=(1, 2))
        tfidf = vec.fit_transform(texts)
        scores = tfidf.sum(axis=0).A1
        terms = vec.get_feature_names_out()
        top_idx = scores.argsort()[::-1][:top_n]
        return [terms[i] for i in top_idx]
    except Exception as exc:
        logger.warning("Keyword extraction failed: %s", exc)
        return []


def auto_label_cluster(keywords: list[str], issues: list[str]) -> str:
    """Generate an auto-label from keywords and extracted issues."""
    # Prefer issue labels from LLM analysis if available
    if issues:
        from collections import Counter

        counter = Counter(issues)
        top = counter.most_common(3)
        return "; ".join(label for label, _ in top)
    if keywords:
        return ", ".join(keywords[:5])
    return "Unlabeled"


def run_clustering(
    submissions: list[dict],  # [{"id": int, "text": str, "issues": list[str]}]
    min_cluster_size: int = 5,
    min_samples: int = 3,
) -> tuple[list[ClusterResult], list[ClusterSummary]]:
    """
    Full clustering pipeline.

    Returns (per-submission cluster results, cluster summaries).
    """
    if not submissions:
        return [], []

    ids = [s["id"] for s in submissions]
    texts = [s.get("text") or "" for s in submissions]
    issues_by_id = {s["id"]: s.get("issues", []) for s in submissions}

    # Embed
    embeddings = embed_texts(texts)

    # Cluster
    labels = cluster_embeddings(
        embeddings, min_cluster_size=min_cluster_size, min_samples=min_samples
    )

    # Build per-submission results
    # Compute soft membership scores as distance to cluster centroid
    cluster_results: list[ClusterResult] = []
    cluster_members: dict[int, list[int]] = {}

    for i, (sid, label) in enumerate(zip(ids, labels)):
        cluster_members.setdefault(label, []).append(i)
        cluster_results.append(
            ClusterResult(
                submission_id=sid,
                cluster_id=int(label),
                relevance_score=0.0,
                is_representative=False,
            )
        )

    # Compute centroids and relevance scores
    centroids: dict[int, np.ndarray] = {}
    for cid, member_indices in cluster_members.items():
        if cid == -1:
            continue
        centroids[cid] = embeddings[member_indices].mean(axis=0)

    for i, result in enumerate(cluster_results):
        cid = result.cluster_id
        if cid == -1 or cid not in centroids:
            result.relevance_score = 0.0
            continue
        centroid = centroids[cid]
        emb = embeddings[i]
        # Cosine similarity to centroid
        denom = np.linalg.norm(centroid) * np.linalg.norm(emb)
        if denom > 0:
            score = float(np.dot(centroid, emb) / denom)
        else:
            score = 0.0
        result.relevance_score = max(0.0, score)

    # Mark representatives (top 5 per cluster by relevance)
    for cid, member_indices in cluster_members.items():
        if cid == -1:
            continue
        cluster_subset = [cluster_results[i] for i in member_indices]
        cluster_subset.sort(key=lambda r: r.relevance_score, reverse=True)
        for r in cluster_subset[:5]:
            r.is_representative = True

    # Build cluster summaries
    cluster_summaries: list[ClusterSummary] = []
    for cid, member_indices in sorted(cluster_members.items()):
        if cid == -1:
            continue
        member_texts = [texts[i] for i in member_indices]
        member_ids = [ids[i] for i in member_indices]
        all_issues = []
        for mid in member_ids:
            all_issues.extend(issues_by_id.get(mid, []))

        keywords = extract_keywords(member_texts)
        auto_label = auto_label_cluster(keywords, all_issues)

        rep_indices = [
            i
            for i in member_indices
            if cluster_results[i].is_representative
        ]
        rep_ids = [ids[i] for i in rep_indices]

        cluster_summaries.append(
            ClusterSummary(
                cluster_id=cid,
                auto_label=auto_label,
                keywords=keywords,
                member_ids=member_ids,
                representative_ids=rep_ids,
                total_count=len(member_ids),
            )
        )

    # Sort by size descending
    cluster_summaries.sort(key=lambda c: c.total_count, reverse=True)
    return cluster_results, cluster_summaries
