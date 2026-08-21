# Greek Common Crawl → FineWeb-style dataset

**Plan v6 — 15 Aug 2026**

*Decisions recorded: per-snapshot deduplication (§3.2); scope = the 27 crawls from CC-MAIN-2024-22 onward, published standalone (§3); Polars for the index scan, PyArrow for the shard writer, Parquet at every read/write boundary (§4.3, §5.5, §5.6).*

---

## Contents

- [1. The one idea that makes this tractable](#1-the-one-idea-that-makes-this-tractable)
- [2. Numbers you should have in your head](#2-numbers-you-should-have-in-your-head)
- [3. Scope and framing](#3-scope-and-framing)
  - [3.1 What you take from FineWeb-2 as a companion rather than a base](#31-what-you-take-from-fineweb-2-as-a-companion-rather-than-a-base)
  - [3.2 Decision: per-snapshot deduplication](#32-decision-per-snapshot-deduplication)
- [4. How to get the data (your question 1)](#4-how-to-get-the-data-your-question-1)
  - [4.1 What steps 1–3 replace (read this if the FineWeb-2 script looks like it already does the job)](#41-what-steps-13-replace-read-this-if-the-fineweb-2-script-looks-like-it-already-does-the-job)
  - [4.2 Access endpoints](#42-access-endpoints)
  - [4.3 Step 1 — Read the columnar index (Polars)](#43-step-1--read-the-columnar-index-polars)
  - [4.4 Step 2 — Dedup *before* you download anything](#44-step-2--dedup-before-you-download-anything)
  - [4.5 Step 3 — Range-fetch the records](#45-step-3--range-fetch-the-records)
  - [4.6 Why not WET files?](#46-why-not-wet-files)
- [5. In-memory filtering, no intermediate state (your question 2)](#5-in-memory-filtering-no-intermediate-state-your-question-2)
  - [5.1 What is and isn't achievable](#51-what-is-and-isnt-achievable)
  - [5.2 The streaming pipeline](#52-the-streaming-pipeline)
  - [5.3 The filter recipe — FineWeb-2's actual Greek config](#53-the-filter-recipe--fineweb-2s-actual-greek-config)
  - [5.4 Per-snapshot deduplication, in one streaming pass](#54-per-snapshot-deduplication-in-one-streaming-pass)
  - [5.5 Output format](#55-output-format)
  - [5.6 Parquet everywhere — no JSONL at any stage](#56-parquet-everywhere--no-jsonl-at-any-stage)
- [6. Where it runs (your question 3)](#6-where-it-runs-your-question-3)
  - [6.1 Can the Raspberry Pi do it?](#61-can-the-raspberry-pi-do-it)
  - [6.2 Remote alternative](#62-remote-alternative)
  - [6.3 Scheduling](#63-scheduling)
- [7. Storage and publishing (your follow-up)](#7-storage-and-publishing-your-follow-up)
- [8. Reprocessing 2013–2024 yourself — probably never](#8-reprocessing-20132024-yourself--probably-never)
- [9. Proposed sequence](#9-proposed-sequence)
- [10. Open questions for you](#10-open-questions-for-you)
- [Sources](#sources)

---

## 1. The one idea that makes this tractable

The naive way to get Greek out of Common Crawl is to download every WARC file and throw away 99.4% of it. That is ~100 TiB per monthly crawl, and it is what forces everyone else onto a Spark cluster.

You don't have to. Since **CC-MAIN-2018-39**, Common Crawl ships a **columnar (Parquet) index** with a `content_languages` column (CLD2-detected, ISO-639-3), alongside `warc_filename`, `warc_record_offset` and `warc_record_length`. So you can:

1. Read a few columns of the index (tens of GB, not 100 TiB),
2. Filter to the rows whose `content_languages` contains `ell` — locally, in Polars,
3. Fetch **only those WARC records** via HTTP/S3 range requests.

That turns ~100 TiB/crawl into roughly **540 GB/crawl** of actual content download — a ~180× reduction. This is the difference between "needs a cluster" and "a Raspberry Pi could plausibly do it."

Everything below builds on that.

---

## 2. Numbers you should have in your head

Based on CC-MAIN-2026-12 (March 2026) as a representative recent crawl:

| Quantity | Value |
|---|---|
| Pages per monthly crawl | **1.97 B** |
| Uncompressed content per crawl | 344.6 TiB |
| WARC files per crawl | 100,000 (100 segments, ~1 GB each) |
| Avg compressed WARC record | ~50 KB |
| URLs new (not in any prior crawl) | 600 M (**30%** — so 70% are re-visits) |
| Greek share of pages (`ell`) | **~0.52–0.56%** (stable across 2026 crawls) |
| ⇒ **Greek pages per crawl** | **~10–11 M** |
| ⇒ **Greek bytes to fetch per crawl** | **~540 GB** |
| Greek records per 1 GB WARC file | ~108 (out of ~19,700) |
| Columnar index, all 26 columns | ~300 GB/crawl |
| Columnar index, **the 6 columns we need** | **~40–50 GB/crawl** (estimate — verify empirically) |

Downstream, for calibration: **FineWeb-2's `ell_Grek`** is 47.4 M documents / 22.8 B words / 222 GB UTF-8 (73 GB on disk) from 96 crawls — but that's after *global* dedup. You're doing **per-snapshot** dedup (§3.2), which in FineWeb v1's English ablations yielded roughly **5× more tokens** than the global equivalent. Scaling that: per-snapshot over 96 crawls would be ~240 M docs, i.e. ~2.5 M docs per crawl. Over your **27 crawls** expect roughly **60–100 M documents, ~100–150 GB of Parquet**. Comfortably inside an HF PRO plan. Storage is not your problem. **Transfer and CPU are.**

---

## 3. Scope and framing

You want to start where FineWeb-2 stopped and do the rest your way. Three facts shape what that means:

**(a) There is no language annotation before CC-MAIN-2018-39.** Common Crawl added CLD2 language detection in Aug/Sep 2018; the [erratum](https://commoncrawl.org/errata/missing-language-classification) states everything from CC-MAIN-2008-2009 through CC-MAIN-2018-34 lacks it. For those ~55 older crawls the core trick in §1 does not work, and you'd need a much more expensive approach (§8).

**(b) FineWeb-2 already covers 2013 → April 2024 for Greek**, under ODC-By — so on the face of it, reprocessing that era duplicates existing work. That would have argued for starting at CC-MAIN-2024-22 and appending. But:

**(c) You have chosen per-snapshot deduplication (§3.2), which means you cannot *merge into* `ell_Grek`.** Their corpus is globally deduped and the dedup-removed documents were never published, so it can't be converted to a per-snapshot corpus. But this does **not** force you to rebuild 2013–2024 — see below.

**Scope: the 27 crawls CC-MAIN-2024-22 → CC-MAIN-2026-30**, oldest → newest. FineWeb-2 stops where April 2024 stops; you pick up from May 2024 and do the rest your way.

**Publish it as a standalone dataset, not as an extension of FineWeb-2.** This is the framing that makes everything consistent. You are not appending rows to their corpus; you are publishing a separate, clearly-labelled dataset with its own dedup semantics, which a user can combine with `ell_Grek` if they choose.

One property belongs in the dataset card as a note rather than a warning: a user who concatenates the two gets a page from the historical era once (FineWeb-2 globally deduped it) and a page from your era up to 27 times (per-snapshot). So the recent era is upweighted relative to the historical one. That's a documented property, not a bug — and arguably a defensible one for a corpus meant to reflect the current Greek web. Anyone who wants uniform semantics across 2013→now would have to reprocess the old era themselves, which is §8.

- **Main track.** CC-MAIN-2024-22 → CC-MAIN-2026-30, oldest → newest, per-snapshot dedup. **27 crawls.**
- **Steady state.** Append each new crawl monthly as CC publishes it (§6.3).
- **Optional, much later.** Reprocess 2013–2024 your way for a uniform corpus. See §8. Not recommended.

Pull the authoritative crawl list from `collinfo.json` at runtime rather than hardcoding it — the scheduler in §6.3 does this anyway, and crawl IDs are irregular (Common Crawl ran 12 crawls in 2019 but only 5 in 2023). Note my two reads of `collinfo.json` differed by ±1 on the boundary crawl; confirm whether FineWeb-2's last snapshot is CC-MAIN-2024-18 and start at the next one.

**Still prove the pipeline on the newest crawl first, not the oldest.** Per-snapshot dedup makes every crawl fully independent (§5.4), so processing order is a free choice. Validate against live Greek websites on a current crawl, *then* run the oldest→newest queue.

### 3.1 What you take from FineWeb-2 as a companion rather than a base

`ell_Grek` is not the foundation of your dataset — it's the volume that sits next to it on the shelf, covering 2013 → April 2024 while yours covers May 2024 →. Three things are worth taking from it:

1. **The Greek filter config** (§5.3) — thresholds and stopwords, validated by their ablations. This is the single biggest gift and it costs nothing.
2. **A quality baseline.** Download `ell_Grek` (73 GB) anyway and use it to sanity-check your output: token-length distributions, domain mix, and — most useful — train a small LM on a matched-size sample of each and compare. If your per-snapshot corpus doesn't at least match theirs, something in your pipeline is broken.
3. **A validation set for the extraction stage.** Their documents came through `Trafilatura(favour_precision=True)` on the same WARCs. Run your extractor on a few thousand URLs that also appear in `ell_Grek` and diff the text — a cheap, strong test that your range-fetch and extraction path is correct.

For reference: `ell_Grek` is 47.4 M documents, 22.8 B words, 73 GB on disk, ODC-By 1.0, covering 2013 → April 2024. There's also an `ell_Grek_removed` config, but note that "removed" means *quality-filter*-removed. Per the repo README they "remove all except one document from each duplicate cluster" and keep only `minhash_cluster_size` — **the dedup-removed documents were never published.** That's why the global→per-snapshot conversion is impossible rather than merely tedious.

**And the pipeline is fully reproducible.** The [`huggingface/fineweb-2`](https://github.com/huggingface/fineweb-2) repo publishes the actual datatrove script plus **per-language configs at `configs/{iso3}_{script}.yml`** — so `configs/ell_Grek.yml` already contains the derived Greek thresholds and stopword list. **Use that file verbatim.** Do not re-derive Greek thresholds; that was the entire research contribution of the FineWeb-2 paper and it's sitting in a YAML file.

Their stage order, for reference — copy all of it except the dedup scope (note dedup runs *before* quality filtering, which is itself a change from FineWeb v1 and worth keeping):

1. `WarcReader → URLFilter → Trafilatura(favour_precision=True) → LanguageFilter`; the *rejected* (non-English) branch is what became FineWeb-2's input
2. `LanguageFilter(backend="glotlid", label_only=True, keep_top_pairs_threshold=0.01)`, written out as `{language}_{script}/{dump}/`
3. `LambdaFilter` enforcing `language_score >= 0.826` (the Greek value from `configs/ell_Grek.yml`)
4. MinHash dedup, 4 sub-stages — **14 buckets × 8 hashes, 5-grams, xxhash 64-bit**, `save_cluster_size=True`
5. `GopherRepetitionFilter → FineWebQualityFilter → GopherQualityFilter`, params from the config. **No C4 filters** — the script says so explicitly
6. `FTFYFormatter → PIIFormatter → SymbolLinesFormatter(symbols_to_remove=["|"], replace_char="\n")` (that last one cleans up trafilatura's table artifacts)

**Your pipeline is this, with step 4's scope changed from per-language-global to per-snapshot.** Everything else stays. That's the whole diff — which is why the decision is cheap to implement even though it's expensive in scope (§3.2).

### 3.2 Decision: per-snapshot deduplication

**Decided: per-snapshot, following FineWeb v1.** Recording the reasoning and the consequences here, because this decision propagates into almost every other section.

The two projects genuinely disagree, and FineWeb-2's README states the split plainly:

> "Unlike in FineWeb, where data was deduplicated per CommonCrawl snapshot, in FineWeb 2, **data is deduplicated per language globally**."

FineWeb v1's evidence for per-snapshot is strong: global dedup across 96 snapshots produced *worse* data, because what survives skews toward "ads, incoherent lists of keywords and generally badly formatted text," and it yielded 4 T tokens against 20 T for per-snapshot. FineWeb-2 went global anyway — but then had to bolt on rehydration to claw the loss back, upsampling documents by cluster size with hand-picked weights (`{1:1, 2:2, 3:3, 5:5, 100:8, 1000:1}`, and their README concedes "we did not extensively explore different upsampling weights").

Which is worth noticing: **rehydration is an approximation of what per-snapshot dedup gives you for free.** A page that persists across 40 snapshots naturally appears 40 times under per-snapshot; global-dedup-plus-upsampling tries to reconstruct that multiplicity from a cluster-size heuristic. Going per-snapshot means taking the real signal instead of a fitted proxy for it. Your instinct here is well-founded.

**Consequences — three of them are wins:**

1. **Bounded state.** This is the big architectural payoff. Global dedup needs a persistent LSH index that grows forever — ~45 GB of signatures by crawl 73, on RocksDB, with cross-crawl ordering dependencies. Per-snapshot dedup means the LSH index is **built and thrown away per crawl**: ~11 M docs × 14 bucket keys ≈ **2.5 GB, in RAM, gone at the end of the crawl.** No persistent dedup store at all. This makes the "no intermediate state" goal in §5 far more achievable and materially improves the Raspberry Pi's odds.
2. **Crawls become independent.** No ordering constraint, no cross-crawl resume logic, trivially parallel across machines, and a failed crawl can be redone in isolation. Oldest→newest is now a preference rather than a requirement.
3. **No FineWeb-2 signature bootstrap.** The Phase 5-ish bootstrap task from the previous draft — recomputing MinHash over 47.4 M documents to seed a global index — disappears entirely.

**And one cost, now much smaller than it looked:**

4. **You give up merging into `ell_Grek`.** Your corpus can't be appended to theirs as one homogeneous dataset. But since you're publishing standalone and starting where they stopped (§3), this costs you almost nothing in practice — you were never going to reprocess 2013–2024 anyway. Scope stays at **27 crawls**, roughly **$170 and a day or two** on us-east-1 spot.

**Still record `minhash_cluster_id` and `minhash_cluster_size` per snapshot** (§5.5). Within-snapshot cluster sizes cost almost nothing to emit and let a downstream consumer apply extra dedup or FineWeb-2-style weighting if they want it. Per-snapshot is the default the dataset ships in; it shouldn't be the only option a user has.

---

## 4. How to get the data (your question 1)

### 4.1 What steps 1–3 replace (read this if the FineWeb-2 script looks like it already does the job)

Reasonable objection: their pipeline is public and works, so why is any of §4 here?

Because of the very first block in their script:

```python
WarcReader(
    f"s3://commoncrawl/crawl-data/{DUMP_TO_PROCESS}/segments/",
    glob_pattern="*/warc/*",  # we want the warc files
)
```

That reads **every WARC file in the crawl** — all 100,000 of them, ~100 TiB — runs trafilatura on all 1.97 B pages, and then throws away everything that isn't the language it wants. They provisioned it as `tasks=8000` with a 10-hour wall limit on a Slurm partition. That's the right design *for them*: FineWeb-2 targets 1,868 language-script pairs, so there was nothing to pre-filter on. They had to touch everything anyway.

**You want one language that is 0.55% of the crawl.** That changes the economics completely, and §1's trick only exists because of it. Steps 1–3 are not a reimplementation of their pipeline — they are a **drop-in replacement for that single `WarcReader` block**:

| | FineWeb-2 | You |
|---|---|---|
| Input stage | `WarcReader` over 100 k files | **Step 1** index scan → **Step 2** digest dedup → **Step 3** range fetch |
| Bytes read per crawl | ~100 TiB | **~540 GB** |
| Hardware | 8,000 Slurm tasks | one machine (or a Pi) |
| Everything downstream | `URLFilter → Trafilatura → LanguageFilter → …` | **identical, copied from their script** |

In datatrove terms you write one custom `PipelineStep` — call it `CCIndexGreekReader` — that yields `Document`s exactly as `WarcReader` does, and substitute it. Every block after that point is theirs, unchanged, with `configs/ell_Grek.yml` loaded as-is.

**So what's actually left to build?** Three things, and none of them are filters:

1. **The reader** (§4.3–3.4). Doesn't exist upstream because nobody else needed a single-language slice.
2. **A streaming executor** (§5). Their script is six `SlurmPipelineExecutor` stages, each writing JSONL to S3 and reading it back — six full materialisations of the dataset. You have no Slurm cluster and you asked for no intermediate state, so those six stages collapse into one in-memory pass. (The writers that do survive become Parquet, never JSONL — §5.6.)
3. **Per-snapshot dedup** (§5.4). Their MinHash is four Slurm stages with a disk shuffle, scoped globally. Yours is an online LSH index scoped to one crawl, held in RAM.

The filters, thresholds, stopwords, stage order and formatters are all settled and copied verbatim. What you're building is the plumbing around them.

### 4.2 Access endpoints

Three routes to the same bytes:

| Endpoint | Notes |
|---|---|
| `s3://commoncrawl/…` | **Best.** From an EC2 instance in **us-east-1**, transfer is free. Anonymous (`--no-sign-request`), no requester-pays. CC states it's "mandatory to access the data from the region where it is located (us-east-1)" for S3. |
| `https://data.commoncrawl.org/…` | Works anywhere. CloudFront-fronted. **Rate limited** — CC has had repeated 503/SlowDown incidents from users hammering it (one thread documents 40+ Gbps abuse dropping everyone to 128 kb/s). Be polite: modest concurrency, exponential backoff on 503. |
| `https://ds5q9oxwqwsfj.cloudfront.net/…` | The CloudFront origin, same content. |

If you're renting anyway, **rent in us-east-1** — the free in-region S3 transfer is worth more than the price difference vs. Hetzner.

### 4.3 Step 1 — Read the columnar index (Polars)

Layout: `s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-YYYY-WW/subset=warc/*.parquet`
(HTTPS mirror: `https://data.commoncrawl.org/cc-index/table/cc-main/warc/…`)

**Polars is the right default here**, and the reason is structural rather than aesthetic: the only genuinely large operation in this whole plan is a **projection + filter scan** — no joins, no big group-bys, no shuffles. That is the easiest possible shape for a streaming engine, so the usual "will it spill gracefully?" worry mostly doesn't apply. And it keeps the manifest, the fetch loop and the output writer in one Python process instead of marshalling across a SQL boundary.

```python
import polars as pl

CRAWL = "CC-MAIN-2026-30"
SRC = (
    f"s3://commoncrawl/cc-index/table/cc-main/warc/crawl={CRAWL}/subset=warc/*.parquet"
)

lf = (
    pl.scan_parquet(
        SRC,
        storage_options={"aws_region": "us-east-1", "aws_skip_signature": "true"},
        hive_partitioning=False,
    )
    .filter(
        (pl.col("fetch_status") == 200)
        # exact match on a comma-separated ISO-639-3 list, not a substring test
        & pl.col("content_languages").str.split(",").list.contains("ell")
        & pl.col("content_mime_detected").is_in(["text/html", "application/xhtml+xml"])
    )
    .select(
        "url",
        "url_host_registered_domain",
        "content_digest",
        "content_languages",
        "warc_filename",
        "warc_record_offset",
        "warc_record_length",
    )
)

# the heavy part: streams ~40–50 GB of columns, emits ~10–11 M rows (~1–2 GB)
manifest = lf.collect(engine="streaming")

# small enough to finish in memory — see §4.4 and §4.5
manifest = manifest.sort(["warc_filename", "warc_record_offset"]).unique(
    subset=["content_digest"], keep="first", maintain_order=True
)
manifest.write_parquet("greek_manifest.parquet", compression="zstd")
```

Result: ~10–11 M rows, a **~400 MB manifest**. This is the only thing you persist per crawl besides the output.

**Unverified — check in Phase 1.** I could not run any of this (the environment I drafted it in has no network and no Polars), so treat the following as the specific things to confirm rather than as established fact:

- **Anonymous S3.** The `storage_options` key for unsigned requests comes from the underlying `object_store` crate; `aws_skip_signature` is the name I believe Polars forwards, but confirm. The equivalent AWS CLI behaviour is `--no-sign-request`.
- **`collect(engine="streaming")`** is the current spelling; older Polars used `collect(streaming=True)`. Version-dependent.
- **Peak memory on the scan.** This is the one number that actually matters, especially if the Pi is doing the work — measure RSS across a full crawl's index before trusting the 8 GB figure in §5.2.
- **Globbing over HTTPS.** `s3://` supports listing, so `*.parquet` resolves. Plain HTTPS almost certainly does not — if you run off `data.commoncrawl.org` (which you must on the Pi, since CC asks that S3 access come from us-east-1), you'll need an explicit file list rather than a glob. Work out where that list comes from before relying on it.

**Keep DuckDB as the fallback for this step specifically.** It costs nothing to have both, Common Crawl's own documentation points at it ("DuckDB works well for local SQL queries"), and it is the more trodden path against this particular bucket. The equivalent is a plain `COPY (SELECT … FROM read_parquet('s3://…') WHERE …) TO 'greek_manifest.parquet'` after `INSTALL httpfs; LOAD httpfs; SET s3_region='us-east-1';`. If Polars turns out to blow up on memory or trip over anonymous S3, switching costs an hour.

Two notes on the query itself:

- **`content_languages` is CLD2 and lists up to 3 languages.** Use it as a *high-recall prefilter only*. Matching `ell` anywhere in the list catches multilingual pages where Greek is secondary; the authoritative decision is GlotLID later in the pipeline. Greek's distinct script makes CLD2 recall very high, which is why this trick works so well here (it would be far worse for, say, Bosnian vs Croatian).
- **Drop `url` if index read cost bites.** You can reconstruct it from the WARC record header. `url_host_registered_domain` alone is enough for the domain blocklist prefilter and is far cheaper to read. `content_digest` is the expensive column (high-entropy SHA-1, poorly compressible, ~30 GB/crawl) but it pays for itself — see §4.4.

### 4.4 Step 2 — Dedup *before* you download anything

**Within-crawl: yes, unconditionally.** The `.unique(subset=["content_digest"], keep="first", maintain_order=True)` already in the §4.3 snippet does it — one row per distinct payload, chosen deterministically because the sort runs first. This removes byte-identical duplicates (mirrors, session-id URL variants, print/mobile versions) at zero download cost, and it is squarely inside the per-snapshot regime — it *is* per-snapshot dedup, just the exact-match half of it, done on metadata before any bytes move.

**Across crawls: careful — this is the one place per-snapshot dedup changes the answer.**

The previous draft proposed a persistent Bloom filter of every `content_digest` ever seen, to skip re-downloading unchanged pages. With 70% of URLs being re-visits that's a big bandwidth win. But under a per-snapshot regime it is **not** a free optimisation: a page present in 40 snapshots is *supposed* to appear 40 times in the corpus. That natural multiplicity is exactly the signal FineWeb v1 found valuable and that FineWeb-2 had to approximate with rehydration weights (§3.2). Silently skipping the re-download would quietly convert your corpus to global exact-dedup and throw that signal away — the very thing you chose per-snapshot to avoid.

Two ways to have it both ways:

**Option A — faithful v1 (simplest, recommended if you're on AWS).** Don't do cross-crawl dedup at all. Download and emit every crawl independently. A stable page appears once per snapshot it's in, exactly as in FineWeb v1.
- Cost: ~540 GB/crawl instead of ~200 GB, and output grows to maybe **150–200 GB** of Parquet across the 27 crawls.
- **On AWS us-east-1 the bandwidth is free**, so this costs you essentially nothing extra beyond a somewhat higher GET-request count. On a Pi or Hetzner it roughly triples download time.

**Option B — store-once + occurrence table (lossless, better for the Pi).** Keep the persistent digest store, skip the re-download, but **emit an occurrence row** `(content_digest, crawl, url, fetch_time)` into a small side table instead of dropping the observation. The text is stored once; the multiplicity is recorded.
- A consumer reconstructs the faithful v1 form with a single join, and you can ship both as HF configs (`default` deduped-with-occurrences, `per_snapshot` rehydrated).
- Occurrence rows are ~40 bytes; even 300 M of them is ~10 GB.
- Caveat: this only catches **byte-identical** repeats. A page whose HTML changed by one timestamp gets a different digest and is stored again — correctly, since under per-snapshot regime it's a legitimately distinct observation.

Storage for the digest set under Option B:
- **Scalable Bloom filter** — 400 M digests at 1e-7 FP ≈ 1.7 GB RAM. Fits an 8 GB Pi. Cost: a tiny fraction of documents silently treated as repeats. Acceptable.
- **RocksDB / LMDB on SSD** — exact, ~8–10 GB. Preferable on a rented box.

**Recommendation: Option A for the AWS backfill, Option B if you end up doing significant work on the Pi.** They produce the same corpus; B just trades a join for bandwidth. Note that Option B reintroduces persistent cross-crawl state, which partly gives back the "bounded state" win from §3.2 — the LSH index still stays per-crawl, but the digest store does not.

### 4.5 Step 3 — Range-fetch the records

Each manifest row gives you `(warc_filename, offset, length)`. A WARC file is a concatenation of independently-gzipped members, so **a range read of exactly `[offset, offset+length)` is a complete, independently decompressible gzip member.** No need to touch the rest of the file.

```python
# S3 (us-east-1, free transfer)
resp = s3.get_object(
    Bucket="commoncrawl",
    Key=row.warc_filename,
    Range=f"bytes={row.warc_record_offset}-{row.warc_record_offset + row.warc_record_length - 1}",
)
raw = resp["Body"].read()  # ~50 KB, in RAM
rec = next(ArchiveIterator(io.BytesIO(raw)))  # warcio parses it directly
html = rec.content_stream().read()
```

**Sort the manifest by `(warc_filename, offset)`** before fetching. ~108 Greek records per WARC file, so you get connection reuse and sequential-ish access. Coalesce ranges that are within ~1 MB of each other into a single request — at ~6 MB average gap the win is modest, but free.

Concurrency: on S3, 64–256 in-flight requests is fine. On `data.commoncrawl.org`, stay much lower (8–16) and back off hard on 503.

**Cost note (AWS):** ~11 M range GETs/crawl × $0.0004/1000 ≈ **$4.40/crawl in request charges**, transfer free. Across 27 crawls ≈ $120 — more if you take §4.4 Option A, since you re-fetch stable pages every crawl. Still the dominant AWS line item, and still small.

### 4.6 Why not WET files?

WET files are pre-extracted plain text and only ~8–9 TiB/crawl, which sounds attractive. **Don't.** The FineWeb paper is explicit that WET extraction retains far too much boilerplate — navigation, menus, cookie banners — and that switching to WARC + trafilatura was one of their most impactful quality decisions. WET is a reasonable tool for the pre-2018 problem (§8), not for the main pipeline.

---

## 5. In-memory filtering, no intermediate state (your question 2)

### 5.1 What is and isn't achievable

Let me be straight about this, because it's the part where a plan can quietly lie to you.

- **Everything single-document is trivially streamable.** Extraction, language ID, all the quality heuristics, PII scrubbing — these are pure functions of one document. Zero intermediate state, genuinely.
- **Deduplication is not.** Near-duplicate detection is inherently a global operation. You cannot do it without *some* persistent state.

The honest goal is therefore: **never persist documents; persist only fixed-size metadata.** The manifest (400 MB), the digest set (~2 GB Bloom), and the MinHash LSH index are the only things on disk. Documents go RAM → filters → Parquet buffer → cloud, and are never written locally.

### 5.2 The streaming pipeline

```
manifest.parquet (400 MB, on disk)
   │
   ├─ dedup on content_digest (within-crawl + persistent Bloom)   ← no download
   ├─ URL/domain blocklist (UT1 adult list, FineWeb's list)       ← no download
   │
   ▼  asyncio worker pool, N in flight
range GET → bytes in RAM
   │
   ├─ warcio parse from BytesIO
   ├─ charset detect + decode (use index content_charset as a hint)
   ├─ trafilatura extract          ← the CPU bottleneck, ~85% of cycles
   ├─ GlotLID → keep iff label == 'ell_Grek' and p ≥ threshold
   ├─ Gopher repetition filter     ← Greek-adapted thresholds
   ├─ Gopher quality filter        ← Greek stopword list, Greek tokenizer
   ├─ C4 filters (minus terminal-punctuation)
   ├─ FineWeb quality filters (3 heuristics)
   ├─ PII: regex-anonymise emails + public IPs
   ├─ MinHash signature → online LSH → assign cluster id, DON'T drop
   │
   ▼
pyarrow RecordBatch buffer in RAM
   │  at ~500 MB / 250k docs:
   ▼
write Parquet to io.BytesIO → upload to HF Hub / R2 → free the buffer
```

Peak RSS: on the order of **4–7 GB**, dominated by the per-crawl LSH index (~2.5 GB, §5.4), plus the Arrow buffer, N×50 KB in flight, and worker overhead. An 8 GB Pi 5 is tight-but-workable (spill the LSH index to LMDB if not); 16 GB is comfortable. Note the LSH index is **discarded at the end of each crawl** — memory use is flat across the whole 27-crawl backfill, not growing.

### 5.3 The filter recipe — FineWeb-2's actual Greek config

**Take this from the published code, not from the FineWeb v1 paper.** The v1 recipe (C4 filters, the three custom heuristics at their English thresholds) is *not* what produced FineWeb-2, and copying it would make your data inconsistent with `ell_Grek`. Concretely, FineWeb-2 **does not apply the C4 filters at all** — the script carries the comment `# we do not apply the C4 filters` — and it disables several sub-filters that trafilatura makes redundant.

The real pipeline, with the Greek values from `configs/ell_Grek.yml`:

| Stage | Setting |
|---|---|
| `URLFilter` | blocklist, as in FineWeb v1 |
| `Trafilatura` | `favour_precision=True` |
| `LanguageFilter` | `backend="glotlid"`, `label_only=True`, `keep_top_pairs_threshold=0.01` |
| language score | **`>= 0.826`** for `ell_Grek` |
| `GopherRepetitionFilter` | `dup_line_frac=0.31`; `top_n_grams=[[2,0.194],[3,0.175],[4,0.156]]`; `dup_n_grams=[[5,0.174],[6,0.163],[7,0.153],[8,0.142],[9,0.130],[10,0.119]]`; **`dup_para_frac=0`, `dup_line_char_frac=0`, `dup_para_char_frac=0`** (disabled — trafilatura already strips paragraphs) |
| `FineWebQualityFilter` | `line_punct_thr=0.167`; `new_line_ratio=0.116`; `char_duplicates_ratio=0.1` (raised from 0.01 in FineWeb English); **`short_line_thr=999`** — i.e. the short-line heuristic is **disabled** |
| `GopherQualityFilter` | `max_avg_word_length=11`, `min_avg_word_length=3`, `max_non_alpha_words_ratio=0.809`, `min_stop_words=2`, 21-word Greek stopword list (`του, και, το, της, η, την, από, ο, με, τον, να, που, των, στην, για, στο, σε, είναι, τη, …`) |
| C4 filters | **not applied** |
| Finishing | `FTFYFormatter` → `PIIFormatter` → `SymbolLinesFormatter(symbols_to_remove=["|"], replace_char="\n")` |

Note the ordering again: **dedup runs before quality filtering**, not after.

**Do not hand-tune these thresholds yourself.** Deriving them was the entire research contribution of the FineWeb-2 paper (they compared English-baseline / MeanStd / Quantile / 10Tail / MedianRatio, settling on 10Tail + Wikipedia-Quantile for the FineWeb and Gopher-quality filters and MeanStd-on-CC for Gopher-repetition). Load the YAML and pass it straight into datatrove, exactly as their script does.

**Greek-specific gotchas worth deciding on explicitly:**
- **Greeklish** (Greek written in Latin characters) will be classified as non-Greek by GlotLID and silently dropped. Probably correct for pretraining, but it's a real slice of Greek forum/comment text. Decide consciously.
- **Ancient Greek (`grc`) vs Modern (`ell`)** — GlotLID separates them; CLD2's `ell` does not cleanly. You'll pick up some polytonic/ancient text. Consider keeping it in a separate config rather than mixing.
- **Cypriot Greek and heavily-accented/monotonic-vs-polytonic variation** — no action needed, just don't be surprised.

### 5.4 Per-snapshot deduplication, in one streaming pass

This is where the per-snapshot decision pays off architecturally.

Standard datatrove MinHash is **four separate stages** (signature → buckets → cluster → filter), each writing to disk, each a full pass over the data. That's fine on a Slurm cluster and exactly wrong here. But because dedup is now scoped to a single crawl, the entire index fits in memory and you can do it online.

**Sizing the state.** Per crawl you have ~10–11 M Greek documents. A full signature is 14 × 8 = 112 hashes × 8 bytes = 896 B/doc, which would be ~10 GB — too much. You don't need it: for LSH you only need the **14 bucket keys** (each the 8 hashes of one band, hashed to 64 bits). That's 112 B/doc, and the index is 14 hashmaps of `bucket_key → cluster_id`:

```
11 M docs × 14 buckets × (8 B key + 8 B cluster_id) ≈ 2.5 GB
```

In RAM on a rented box, no question. On an 8 GB Pi it's tight but workable — and if it isn't, spill to an LMDB that you **delete at the end of every crawl**. Either way the state is *bounded by one crawl*, never grows, and needs no cross-crawl coordination.

**The online algorithm.** For each streamed document:

1. Compute the 112 hashes over Greek 5-grams; reduce to 14 bucket keys. Pass `language="ell_Grek"` so datatrove picks the right word tokenizer — their script flags this as easy to get wrong, and a whitespace tokenizer on Greek will silently degrade your n-grams.
2. Probe the 14 buckets. On any collision, the document is a near-duplicate: assign it the colliding document's `cluster_id` and increment that cluster's counter.
3. On no collision, mint a new `cluster_id` and insert into all 14 buckets.
4. Emit the document either way (see below).

Never re-read a document. Never write one to local disk.

**Determinism.** Online greedy dedup is order-dependent: which member of a cluster is the "first" one depends on arrival order. Offline clustering picks a canonical member deterministically; you don't have that. Fix it by **processing the manifest in a fixed sort order** — `(warc_filename, warc_record_offset)` is the natural choice since §4.5 sorts by it anyway for fetch locality. Same crawl, same code, same output. Record the sort key in the dataset card so the run is reproducible.

**Emit cluster metadata rather than dropping.** Keep one document per cluster in the `default` config, but write `minhash_cluster_id` and `minhash_cluster_size` on it, and ship the dropped near-dupes in a `_removed` config with their cluster ids. Reasons:

- It costs almost nothing and makes the dataset auditable — a reader can check what your dedup actually did.
- It leaves the door open to FineWeb-2-style rehydration for anyone who wants it. Their published weights, for reference: `{1:1, 2:2, 3:3, 5:5, 100:8, 1000:1}`, applied by bisect on `minhash_cluster_size`.
- It costs you nothing in the default reading of the dataset, which is the per-snapshot corpus you actually want.

Note the asymmetry with global dedup: because clusters are per-snapshot, `minhash_cluster_size` here means "how many near-identical copies existed *within this crawl*", not "across the web's history". Document that clearly in the dataset card — it's the same column name as FineWeb-2 with a different meaning, which is a trap for anyone mixing the two.

Exact-digest duplicates you drop earlier and more cheaply, at §4.4, before any download.

### 5.5 Output format

Parquet, zstd, ~500 MB shards, partitioned `crawl=CC-MAIN-YYYY-WW/`.

**On the writer: use PyArrow here, not Polars.** This is the one place in the pipeline where Polars is the weaker fit. The shape of the work is "accumulate documents one at a time, flush a shard every ~250 k rows" — that's incremental `RecordBatch` appending, which is exactly what `pyarrow.RecordBatchBuilder` / `ParquetWriter` is for. Building a `pl.DataFrame` per flush means materialising the whole shard as columns first, which is more copying for no benefit. Polars earns its place on the *scan* side (§4.3) and for any QA or analysis you do over the finished shards; PyArrow earns it here. Both wrap the same Arrow memory, so there's no conversion cost at the boundary.

Schema:

| column | notes |
|---|---|
| `text` | extracted, filtered, PII-scrubbed |
| `id` | uuid or `warc_filename:offset` |
| `url`, `url_host_registered_domain` | |
| `date` | crawl fetch time |
| `crawl` | e.g. `CC-MAIN-2026-30` |
| `content_digest` | CC's SHA-1, for provenance + re-dedup |
| `language`, `language_score` | GlotLID output |
| `minhash_cluster_id`, `minhash_cluster_size` | **scoped to this crawl only** — see the warning below |
| `token_count` | precompute it; everyone wants it |

**Warning to put in the dataset card:** `minhash_cluster_size` here means *"near-identical copies within this one crawl"*. FineWeb-2 uses the same column name to mean *"across all 96 snapshots"*. Anyone mixing the two datasets, or applying FineWeb-2's rehydration weights to yours, will get nonsense. Name the difference loudly.

Also emit:

- A **`_removed` sibling config** — same schema plus `removal_reason` (`url_blocklist` / `lang_score` / `goph_rep` / `fw_qual` / `goph_qual` / `minhash`), at least sampled. FineWeb-2 publishes theirs and it's what makes a dataset auditable rather than a black box.
- If you take **Option B** in §4.4, an **`occurrences` config**: `(content_digest, crawl, url, fetch_time)`. Small, ~40 B/row, and it's what lets a consumer reconstruct the faithful per-snapshot multiplicity with one join.

### 5.6 Parquet everywhere — no JSONL at any stage

**Policy: every reader and writer in this project is Parquet.** FineWeb-2's script uses `JsonlWriter` between all six stages and on every `exclusion_writer`; that made sense for them (cheap append-only intermediates on cluster storage) and makes none for you. Two reasons it's the wrong default here:

- **Most of their writers disappear anyway.** The six inter-stage materialisations collapse into one streaming pass (§5.2), so there is nothing to write between stages. What survives is the manifest, the final shards, and the exclusion writers.
- **Everything that does survive is columnar-shaped.** The manifest gets scanned with projection pushdown (§4.3), the shards get read by `datasets`/Polars/DuckDB, and JSONL forfeits column pruning, predicate pushdown, dictionary encoding and typed nulls for no gain.

datatrove ships both classes, so it's close to a find-and-replace:

```python
from datatrove.pipeline.readers import ParquetReader
from datatrove.pipeline.writers import ParquetWriter

ParquetWriter(
    output_folder=f"{OUT}/removed/goph_qual/",
    compression="zstd",  # class default is "snappy" — override it
    batch_size=10_000,  # default 1000 → too many tiny row groups
    expand_metadata=True,  # flatten metadata into real columns, not a struct
    max_file_size=512 * 2**20,  # 512 MB shards; class default is 5 GB
)
```

Four things worth setting deliberately rather than accepting the defaults:

| param | default | use | why |
|---|---|---|---|
| `compression` | `"snappy"` | `"zstd"` | Meaningfully smaller for archival text; you're storing this for years. |
| `batch_size` | `1000` | `10_000` | Row-group size. Exclusion writers trickle a few docs at a time, and 1,000-row groups on a slow-filling stream produce a badly fragmented file. |
| `expand_metadata` | `False` | `True` | Without it, `language_score`, `minhash_cluster_size` etc. land inside a nested `metadata` struct instead of as top-level columns. Consumers will thank you. |
| `max_file_size` | `5 GB` | `512 MB` | Matches the ~500 MB shard target above and keeps HF commits manageable. |

Leave `use_content_defined_chunking=True` (the default) alone — it's Xet-friendly chunking, which means re-uploading a corrected shard to the Hub only transfers the changed chunks (§7).

`ParquetReader` takes the mirror-image arguments (`data_folder`, `glob_pattern`, `batch_size`, `text_key`, `id_key`, `adapter`), so anywhere their script has `JsonlReader(path)` you write `ParquetReader(path)` and nothing downstream notices — both yield the same `Document` objects.

**One caveat.** Parquet is columnar and batch-oriented, JSONL is append-friendly. A writer that is killed mid-shard loses the unflushed batch, whereas a JSONL file truncated mid-stream is still readable up to the last complete line. This is a non-issue for the main output (§6.2 checkpoints at WARC-file granularity, so you replay the shard) but worth knowing if you ever debug a crashed run and find the last Parquet file short.

---

## 6. Where it runs (your question 3)

### 6.1 Can the Raspberry Pi do it?

**Per crawl, on a Pi 5 (4× Cortex-A76 @ 2.4 GHz, 8–16 GB):**

| Stage | Estimate |
|---|---|
| Index scan (~40–50 GB read, Parquet decode) | 2–5 h, network-bound |
| WARC fetch, ~150–250 GB after digest dedup | 3–11 h @ 50–200 Mbit/s |
| trafilatura + filters, ~10 M docs @ ~100 docs/s total | **~28 h — the bottleneck** |
| **Total** | **~2 days per crawl** |

**Verdict:**

- ✅ **Steady state (one new crawl per month): yes, comfortably.** Two days of work per month is well within a Pi's duty cycle. Per-snapshot dedup helps here: state is bounded per crawl and discarded afterwards, so the Pi never accumulates a growing index it can't hold. This is a legitimate, permanent home for the ongoing pipeline.
- 🟡 **Backfill (27 crawls): now genuinely borderline, where at 73 crawls it was hopeless.** ~2 months of continuous grinding and **6–15 TB of egress** (the high end if you take §4.4 Option A). Spread over two months that's ~250 GB/day, about 22 Mbit/s sustained — feasible on a decent home line, and polite enough to Common Crawl if you keep concurrency low and back off on 503s. So: **yes, the Pi can do the whole thing**, which is what you originally asked. It just takes two months instead of a day, and AWS does it for ~$170. Your call which you value.

Pi caveats: use an **NVMe HAT, not an SD card** (an LMDB spill on SD will kill the card); expect to build `fasttext` from source for aarch64; `lxml`/`trafilatura` wheels for ARM64 exist but check; active cooling required for sustained 4-core load. If RAM is the binding constraint, prefer the **16 GB Pi 5** — the 2.5 GB LSH index is the single largest allocation.

### 6.2 Remote alternative

**Recommended: AWS spot in us-east-1** — the free in-region S3 transfer dominates every other consideration.

| | |
|---|---|
| Instance | `c7i.8xlarge` (32 vCPU) or `c7g.8xlarge` (Graviton, cheaper) spot |
| Throughput | ~2,000 docs/s ⇒ **~1.5–2 h per crawl** |
| Spot cost | ~$0.50–0.70/h ⇒ **~$1.20/crawl** |
| S3 GET requests | ~$4.40/crawl |
| Data transfer | **$0** (in-region) |
| **27-crawl backfill** | **≈ $170 total, ~2 days wall-clock** (or a few hours across parallel instances) |

The free in-region transfer is what makes §4.4 **Option A** (no cross-crawl dedup, faithful FineWeb v1 semantics) essentially free on AWS — you pay a somewhat higher GET-request count and nothing else. That's the main reason to do the backfill here rather than on Hetzner.

Because crawls are fully independent (§3.2), you can also **run several instances in parallel, one crawl each**, and cut two days to a few hours. There's no shared dedup state to coordinate. Just don't do it via `data.commoncrawl.org`.

Make it interruption-safe: checkpoint at WARC-file granularity, keep all state in S3, and a spot reclaim costs you one shard.

**Alternative: Hetzner dedicated** (e.g. AX52, 16 threads, ~€50/mo, 1 Gbit/s unmetered). ~1 crawl/day via `data.commoncrawl.org`, so ~1 month and ~€50 for the backfill. Cheaper in absolute terms, but you're pulling tens of TB through CC's CDN instead of staying in-region, with the rate-limit risk that implies — and you'd want §4.4 Option B to keep the bandwidth down, which reintroduces persistent state. **I'd use AWS for the backfill and Hetzner or the Pi for steady state.**

### 6.3 Scheduling

**Steady state — Pi, systemd timer** (not cron; you get logging, `RuntimeMaxSec`, and restart semantics):

```ini
# /etc/systemd/system/ccgreek.timer
[Timer]
OnCalendar=daily
RandomizedDelaySec=6h
Persistent=true
```

The daily job is a **poll, not a run**: fetch `https://index.commoncrawl.org/collinfo.json`, compare against a local `state.sqlite` of completed crawls, and if there's an unprocessed crawl, claim it and start. Otherwise exit in a second. CC publishes roughly monthly, so ~29 of 30 invocations are no-ops.

State machine per crawl, in SQLite: `pending → indexing → fetching → done`, with a `last_completed_warc_file` cursor so a power cut resumes rather than restarts. Take a lockfile so a long-running job doesn't get double-started.

**Backfill — same binary, different queue.** Feed it an explicit ordered list of crawl IDs instead of "newest unprocessed." Same state machine, same checkpoints, so you can run backfill on the rented box and steady-state on the Pi against the same schema, and they won't collide as long as they claim different crawl IDs.

Uploads: never leave completed work sitting locally. Upload each shard as it's finished and record it in `state.sqlite`.

---

## 7. Storage and publishing (your follow-up)

You're right that this has to live in the cloud, but the number is smaller than you'd think — **the ~15 TB is transient transfer, not storage.** The output is ~100–150 GB of Parquet.

**Hugging Face Hub** is the natural home and the numbers work:

| | Public storage |
|---|---|
| Free | best-effort beyond a few GB |
| PRO ($9/mo) | up to **10 TB** |
| Add-ons | $12/TB/mo (1 TB), $10/TB/mo (50 TB) |

Repo guidance: <100 k files per repo, <10 k per folder, <200 GB per file (500 GB hard limit), <100 files per commit. Parquet is explicitly recommended. At 500 MB shards and ~150 GB total you're at ~300 files — nowhere near any limit. **A PRO account (10 TB public) covers this entire project with room to spare.** If it grows, HF grants storage for datasets with demonstrated community value (`datasets@huggingface.co`) — a well-executed Greek corpus is a plausible candidate.

Upload directly from the pipeline with `huggingface_hub.HfApi.upload_file` from an in-memory buffer, or `CommitScheduler` for incremental commits. Xet-backed storage dedups chunks server-side, which helps if you ever re-upload a corrected shard.

**Publish under ODC-By 1.0**, same as FineWeb/FineWeb-2 — it's what the upstream data expects and it keeps you compatible for concatenation.

If you want a working store separate from the published artifact (e.g. for the `_removed` subset, or raw pre-dedup output), **Cloudflare R2 is the right second tier** — zero egress fees means you can re-read it for a global dedup pass later without a bill. S3 in us-east-1 is the alternative if you're already running there.

---

## 8. Reprocessing 2013–2024 yourself — probably never

You've scoped this out, and the plan no longer depends on it. Keeping the notes in case you ever want a uniform 2013→now corpus under one dedup regime.

Two obstacles, in order of severity:

**No `content_languages` before CC-MAIN-2018-39**, so the core trick in §1 doesn't work for 2013–2018. Options there, worst to best: full WARC processing (100 TiB/crawl × ~55 crawls — needs a real cluster); or a **WET-scan then WARC-refetch** — stream WET files (~8–9 TiB/crawl, 10× cheaper), run cheap LID over the plain text to find Greek URLs, look them up in the CDXJ index (covers all crawls back to 2013, carries offsets), and range-fetch just those records for proper trafilatura extraction. Before assuming either, **check whether the columnar index has been backfilled** — in 2018 it held only ~12 crawls and CC said they'd consider extending it. If `content_languages` now exists for older crawls, this obstacle disappears entirely.

**2018-39 → 2024-17 is ~47 crawls** and needs no special handling — it's just §4 applied 47 more times, roughly $300 and a week on us-east-1. That's the cheap half of the problem.

The honest reason not to do it: FineWeb-2 already covers that era competently, and a user who wants it can download `ell_Grek` alongside yours. Reprocessing buys you regime uniformity and nothing else.

---

## 9. Proposed sequence

| Phase | What | Rough time |
|---|---|---|
| **1** | Single-crawl spike on one WARC file. Prove: Polars index scan (anonymous S3, streaming engine, peak RSS), range fetch, warcio parse, trafilatura, GlotLID. Measure real docs/s and real index bytes. Keep a DuckDB fallback for the scan (§4.3). | a day |
| **2** | Full pipeline on **one recent** crawl end-to-end (not the oldest — you want output you can eyeball against live Greek sites), output to a private HF repo. Load `configs/ell_Grek.yml`. Manually review 200 documents. | ~1 week |
| **3** | Validate against FineWeb-2 (§3.1): diff your extraction on a few thousand URLs that also appear in `ell_Grek`. This catches range-fetch and extraction bugs that eyeballing won't. | ~2 days |
| **4** | Add the per-crawl online LSH (§5.4), deterministic manifest ordering, `state.sqlite`, and resume. Rerun phase 2's crawl and confirm byte-identical output. | ~1 week |
| **5** | **Backfill** the 27 crawls CC-MAIN-2024-22 → CC-MAIN-2026-30, oldest → newest. On us-east-1 spot, or on the Pi if you'd rather trade two months for $170. Crawls are independent, so parallelise freely. Publish v1. | ~2 days machine, ~$170 |
| **6** | Deploy steady-state on the Pi. systemd timer, monthly append, auto-upload. | ~2 days |
| **7** | Dataset card, `_removed` config, benchmark vs FineWeb-2 `ell_Grek` by training a small LM on matched-size samples. | ~1 week |
| **8** | Optional: quality classifier (FineWeb-Edu-style) for a `greek-edu` subset; the pre-2018-39 era (§8). | open-ended |

Note what these phases are *not*: none of them involve designing filters or tuning thresholds. That work is done and copied from `configs/ell_Grek.yml` (§3.1, §5.3). Phases 1–4 build the reader, the streaming executor and the per-snapshot dedup — the three things §4.1 identifies as missing from the upstream script. Phase 2 takes a week because range-fetching 11 M records reliably, with retries and resume, is fiddlier than it looks — not because the pipeline is unclear.

The FineWeb-2 signature bootstrap from the previous draft (recomputing MinHash signatures over FineWeb-2 to seed a global index) is **gone** — per-snapshot dedup removes the need for it entirely.

**Do not skip Phase 1.** Every number in §2 marked "estimate" — especially the index read size, the per-crawl LSH memory, and Pi docs/s — should be replaced with a measurement before you commit to a multi-month plan.

---

## 10. Open questions for you

1. **§4.4: Option A (faithful v1, no cross-crawl dedup) or Option B (store-once + occurrence table)?** I lean A for the AWS backfill — it's simpler, and the bandwidth it costs is free in-region. B is better if the Pi ends up doing real work. This is reversible; A can be converted to B later, though not vice versa.
2. **Greeklish and Ancient Greek — in, out, or separate config?** GlotLID will drop Greeklish silently and will separate `grc` from `ell`. Worth a deliberate call.
3. **Do you have an AWS account**, or should the backfill target Hetzner? (Affects question 1.)
4. **HF PRO account?** Needed for anything beyond a few GB of public storage; ~100–150 GB of output fits the 10 TB PRO allowance many times over.
5. **Dataset name?** It ships as a standalone companion to `ell_Grek`, so the name should signal both the continuation (May 2024 →) and the different dedup regime. The `minhash_cluster_size` meaning-collision (§5.5) makes a clearly distinct name worth having.

---

## Sources

- [FineWeb paper (arXiv 2406.17557)](https://arxiv.org/pdf/2406.17557)
- [FineWeb2: One Pipeline to Scale Them All (arXiv 2506.20920)](https://arxiv.org/html/2506.20920v1)
- [HuggingFaceFW/fineweb-2 dataset card](https://huggingface.co/datasets/HuggingFaceFW/fineweb-2)
- [huggingface/fineweb-2 — pipeline code and per-language configs](https://github.com/huggingface/fineweb-2)
- [fineweb-2-pipeline.py](https://github.com/huggingface/fineweb-2/blob/main/fineweb-2-pipeline.py)
- [Common Crawl collinfo.json — authoritative crawl list](https://index.commoncrawl.org/collinfo.json)
- [HuggingFaceFW/fineweb dataset card](https://huggingface.co/datasets/HuggingFaceFW/fineweb)
- [Common Crawl — Columnar Index](https://commoncrawl.org/columnar-index)
- [Common Crawl — Index to WARC Files and URLs in Columnar Format](https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format)
- [Common Crawl — URL Index](https://commoncrawl.org/url-index)
- [Common Crawl — Get Started](https://commoncrawl.org/get-started)
- [Common Crawl — Erratum: Missing Language Classification](https://commoncrawl.org/errata/missing-language-classification)
- [Common Crawl — August 2018 Crawl Archive Introduces Language Annotations](https://commoncrawl.org/blog/august-2018-crawl-archive-now-available)
- [Common Crawl — March 2026 Crawl Archive](https://commoncrawl.org/blog/march-2026-crawl-archive-now-available)
- [Common Crawl Language Statistics](https://commoncrawl.github.io/cc-crawl-statistics/plots/languages)
- [commoncrawl/cc-index-table](https://github.com/commoncrawl/cc-index-table)
- [Common Crawl group — rate limits / 503s](https://groups.google.com/g/common-crawl/c/BvMGYUY-dro)
- [Common Crawl group — columnar index crawl coverage](https://groups.google.com/g/common-crawl/c/escvXUhp3K4)
- [Hugging Face Hub — Storage limits](https://huggingface.co/docs/hub/main/storage-limits)