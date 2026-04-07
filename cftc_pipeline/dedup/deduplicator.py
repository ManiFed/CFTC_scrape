"""Three-pass deduplication: exact hash, MinHash near-dup, campaign detection."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Optional

from cftc_pipeline.config import settings
from cftc_pipeline.extraction.text_cleaner import normalize_for_dedup, word_ngrams

logger = logging.getLogger(__name__)


@dataclass
class DedupeResult:
    submission_id: int
    content_hash: str
    group_id: Optional[int]
    is_canonical: bool
    dedup_method: str  # exact / near_duplicate / campaign / unique
    similarity_score: float


def _sha256_text(text: str) -> str:
    normalized = normalize_for_dedup(text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _make_minhash(text: str, num_perm: int):
    from datasketch import MinHash

    m = MinHash(num_perm=num_perm)
    for gram in word_ngrams(normalize_for_dedup(text), n=3):
        m.update(gram.encode("utf-8"))
    return m


def run_deduplication(
    submissions: list[dict],  # [{"id": int, "text": str}]
) -> list[DedupeResult]:
    """
    Run all three dedup passes on a list of submissions.

    Returns one DedupeResult per submission.
    """
    from datasketch import MinHashLSH

    num_perm = settings.minhash_num_perm
    threshold = settings.minhash_threshold
    campaign_min = settings.campaign_min_size

    results: dict[int, DedupeResult] = {}
    hash_to_group: dict[str, int] = {}
    group_counter = 0
    groups: dict[int, list[int]] = {}  # group_id -> [submission_id]

    # --- Pass 1: Exact hash ---
    logger.info("Pass 1: exact hash deduplication")
    for sub in submissions:
        sid = sub["id"]
        text = sub.get("text", "") or ""
        h = _sha256_text(text)

        if h in hash_to_group:
            gid = hash_to_group[h]
            groups[gid].append(sid)
            results[sid] = DedupeResult(
                submission_id=sid,
                content_hash=h,
                group_id=gid,
                is_canonical=False,
                dedup_method="exact",
                similarity_score=1.0,
            )
        else:
            group_counter += 1
            gid = group_counter
            hash_to_group[h] = gid
            groups[gid] = [sid]
            results[sid] = DedupeResult(
                submission_id=sid,
                content_hash=h,
                group_id=gid,
                is_canonical=True,
                dedup_method="unique",
                similarity_score=1.0,
            )

    # --- Pass 2: MinHash near-duplicate ---
    logger.info("Pass 2: MinHash LSH near-duplicate detection")
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    minhashes: dict[int, object] = {}

    # Only run on submissions that are currently canonical (one per exact group)
    canonical_sids = [sid for sid, r in results.items() if r.is_canonical]

    for sid in canonical_sids:
        text = next(s["text"] for s in submissions if s["id"] == sid) or ""
        mh = _make_minhash(text, num_perm)
        minhashes[sid] = mh
        try:
            lsh.insert(str(sid), mh)
        except Exception:
            pass  # duplicate key in LSH is OK

    for sid in canonical_sids:
        mh = minhashes[sid]
        neighbors = lsh.query(mh)
        neighbors = [int(n) for n in neighbors if int(n) != sid]

        for neighbor_sid in neighbors:
            # Both are canonical — merge into the earlier group
            r_self = results[sid]
            r_neighbor = results[neighbor_sid]

            if r_self.group_id == r_neighbor.group_id:
                continue  # already in same group

            # Merge neighbor's group into self's group
            old_gid = r_neighbor.group_id
            new_gid = r_self.group_id

            for member_sid in groups.get(old_gid, []):
                groups[new_gid].append(member_sid)
                old_r = results[member_sid]
                results[member_sid] = DedupeResult(
                    submission_id=member_sid,
                    content_hash=old_r.content_hash,
                    group_id=new_gid,
                    is_canonical=False,
                    dedup_method="near_duplicate",
                    similarity_score=old_r.similarity_score,
                )
            groups.pop(old_gid, None)

            # Mark neighbor as non-canonical
            results[neighbor_sid] = DedupeResult(
                submission_id=neighbor_sid,
                content_hash=results[neighbor_sid].content_hash,
                group_id=new_gid,
                is_canonical=False,
                dedup_method="near_duplicate",
                similarity_score=0.9,  # approximate; LSH doesn't give exact jaccard
            )

    # --- Pass 3: Campaign detection ---
    logger.info("Pass 3: campaign/form-letter detection")
    for gid, members in groups.items():
        if len(members) >= campaign_min:
            for sid in members:
                old = results[sid]
                if old.dedup_method in ("exact", "near_duplicate"):
                    results[sid] = DedupeResult(
                        submission_id=sid,
                        content_hash=old.content_hash,
                        group_id=gid,
                        is_canonical=old.is_canonical,
                        dedup_method="campaign",
                        similarity_score=old.similarity_score,
                    )

    # Set one canonical per group (prefer longest text)
    text_by_id = {s["id"]: (s.get("text") or "") for s in submissions}
    for gid, members in groups.items():
        if len(members) <= 1:
            continue
        # Choose canonical = longest text in group
        canonical_sid = max(members, key=lambda sid: len(text_by_id.get(sid, "")))
        for sid in members:
            old = results[sid]
            results[sid] = DedupeResult(
                submission_id=sid,
                content_hash=old.content_hash,
                group_id=gid,
                is_canonical=(sid == canonical_sid),
                dedup_method=old.dedup_method,
                similarity_score=old.similarity_score,
            )

    return list(results.values())
