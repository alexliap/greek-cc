"""Online per-snapshot MinHash near-dup tagging (plan_of_action.md §5.4).

Every document is tagged with minhash_cluster_id/minhash_cluster_size and
always emitted -- unlike datatrove's own MinhashDedup* pipeline, which is an
offline multi-stage process (signature files -> buckets -> cluster -> filter)
that *drops* documents. This reuses the same hashing MinhashDedupSignature
uses (14 buckets x 8 hashes x 5-grams, xxhash 64-bit -- FineWeb-2's own
numbers, confirmed against their actual pipeline script) via its
get_shingles()/get_signature() methods, just applied online against an in-RAM
dict index instead of writing signature files to disk. The index -- and the
whole per-run document buffer needed to backfill final cluster sizes -- lives
only for one crawl's extraction run, discarded after ("in RAM, per crawl" per
the plan). Buffering the full run in memory is fine at the bounded sample
sizes this pipeline runs at today; revisit before pointing this at a full
~10M-row crawl.
"""

from collections import Counter

from datatrove.data import Document, DocumentsPipeline
from datatrove.pipeline.base import PipelineStep
from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupSignature
from datatrove.utils.hashing import HashConfig

MINHASH_CONFIG = MinhashConfig(
    hash_config=HashConfig(hash_fc="xxhash", precision=64),
    num_buckets=14,
    hashes_per_bucket=8,
    n_grams=5,
)


class OnlineMinhashDedup(PipelineStep):
    """Tags each doc with a cluster id from an in-RAM, per-run LSH bucket index."""

    name = "🎯 Online MinHash"
    type = "🫂 - DEDUP"

    def __init__(self, language: str = "ell_Grek"):
        super().__init__()
        # only used for its get_shingles()/get_signature() hashing helpers --
        # .run()/.write() are never called, so output_folder is never touched
        self._sig = MinhashDedupSignature(output_folder=".", config=MINHASH_CONFIG, language=language)
        self._buckets: list[dict[tuple[int, ...], int]] = [{} for _ in range(MINHASH_CONFIG.num_buckets)]
        self._cluster_sizes: Counter = Counter()
        self._next_cluster_id = 0

    def run(self, data: DocumentsPipeline = None, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        docs = list(data)
        for doc in docs:
            with self.track_time():
                self._assign_cluster(doc)
        for doc in docs:
            doc.metadata["minhash_cluster_size"] = self._cluster_sizes[doc.metadata["minhash_cluster_id"]]
            yield doc

    def _assign_cluster(self, doc: Document) -> None:
        shingles = self._sig.get_shingles(doc.text)
        buckets = self._sig.get_signature(shingles)
        keys = [tuple(bucket) for bucket in buckets]

        cluster_id = next(
            (self._buckets[bi][key] for bi, key in enumerate(keys) if key in self._buckets[bi]), None
        )
        if cluster_id is None:
            cluster_id = self._next_cluster_id
            self._next_cluster_id += 1
            for bi, key in enumerate(keys):
                self._buckets[bi][key] = cluster_id

        self._cluster_sizes[cluster_id] += 1
        doc.metadata["minhash_cluster_id"] = cluster_id
