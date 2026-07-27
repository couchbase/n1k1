# Design: CEP — n1k1 as a complex-event-processing / standing-query engine

> A research + positioning note, not a build plan. It argues that n1k1's **MQO / `.multi`**
> substrate (see [DESIGN-prepare.md](DESIGN-prepare.md), "PREPARE++") is already the *hard half*
> of a complex-event-processing engine — many standing rules evaluated cheaply over one data flow
> — and sketches the (smaller, well-seeded) other half: an **unbounded source + continuous emit +
> incremental state**. "CEP" is used deliberately instead of "streaming" (which is overloaded with
> ETL/transport meanings); the interesting thing here is *standing queries reacting to events*, not
> byte transport.

## Status & remaining TODOs

_Research note; no code committed for the CEP direction._

**The claim:** the expensive, differentiated part of a CEP engine — *fan one flow out to N
standing rules and evaluate them at sub-linear cost in N* — is **built and benchmarked** as the
shared-scan MQO stack. What's missing is continuous operation. The gap is real engine work but
each piece grows from something that already exists.

**Remaining (headline, none started):**
- [ ] **Unbounded source** — a `records.Source` that blocks/yields as data arrives instead of
  returning `io.EOF` (tail-follow a file, watch a directory, a socket, a Kafka/Redpanda consumer,
  a Debezium CDC topic).
- [ ] **Continuous emit / triggers** — emit-on-watermark for windowed/aggregate detectors (fused
  filter+project detectors already emit per row, so they're free once the source is unbounded).
- [ ] **Incremental standing state** — generalize the O(N) incremental window fold to long-running
  aggregates; hang state on the spill-backed rhmap.
- [ ] **Event-time + watermarks + out-of-order**, **retention/TTL on unbounded state**, and
  **restart-from-source-offset** (at-least-once; *not* distributed exactly-once — see Positioning).

## Thesis: the MQO substrate is a CEP fabric in batch clothing

PREPARE++ was built for support bundles: compile a **git corpus of SQL++ "detectors"** once, run
it against each GB-scale bundle, `UNION ALL` the findings. Structurally that is *"N standing
queries over one pass of data"* — the defining shape of CEP, evaluated in **batch-over-a-finite-
scan** mode. Re-point the scan at an **unbounded** source and the same machinery is a continuous
standing-query engine. The push-based execution model (`base.Val = []byte` flowing through
`yield`) is the same dataflow model a stream processor uses; n1k1 just happens to hit EOF today.

## What already exists — the MQO ↔ CEP mapping

Every shared-scan lever (all DONE + benchmarked; [DESIGN-prepare.md](DESIGN-prepare.md) §"the four
levers") has a direct CEP twin:

| n1k1 MQO lever | CEP / standing-query equivalent |
|---|---|
| `engine.OpBroadcast` — scan once, tee each `[]byte` row to K inlined filter+project detectors, zero-boxing (decode once) | The canonical "one event → N standing queries" fan-out (tee) |
| `engine.OpBroadcastIndexed` + `base.AhoCorasick` — index each detector by a **necessary** discriminating literal; one pass over raw row bytes wakes only detectors whose literal is present. O(K×rows) → ~O(hits×rows), ~60× at K=1000, **flat in K** | The **rule-discrimination network** (Rete / literal pre-filter) — the single hardest problem in high-fan-in CEP, and n1k1 has a *sound* one (necessary ⇒ over-wake safe, under-wake impossible; guarded by a byte-identical differential test) |
| `BroadcastCSE` — sub-predicates shared across detectors computed once/row (`^cseN` precompute column; expr-identity via canonical marshal) | Multi-query optimization / shared operators (the NiagaraCQ → TelegraphCQ streaming-DB research lineage) |
| `BroadcastRoute` — a source fans out only to detectors that `FROM` it | Per-stream routing of a multiplexed event bus |
| Window functions `OVER (ROWS/RANGE/GROUPS …)` + the **incremental fold** (left-anchored O(N) carry; invertible add-enter/remove-leave for COUNT) | Sliding / tumbling / session windows with incremental aggregation — the seed of incremental view maintenance |
| ASOF / nearest-preceding merge ([DESIGN-merging.md](DESIGN-merging.md)) — stock correlated-argmax subquery lowered to an O(n) merge | Temporal / as-of stream join (correlate an event with the latest prior state) |
| Late binding: logical→physical per bundle at EXECUTE, fail-loud on empty | Connector/source binding; "rebind is data, not code" — the compiled MQO structure is bind-invariant |
| `CorpusCompile` fuse / standalone / reject classification + `gate:` preconditions | A planner deciding which rules share the scan vs run their own operator, and cheap guards to skip idle rules |
| `.multi run/lint/test/list`, git corpus, golden-fixture CI, the **lint report card** | **Detections-as-code** with a correctness + efficiency oracle (see Agentic angle) |

The load-bearing point: the **predicate index is the crown jewel for CEP.** "N standing rules × M
events/sec" is precisely the problem it already solves, and its cost is ~flat in N. Most of a CEP
engine's value is *making N large cheaply*; that part is done.

## The gap to continuous operation (ranked by lift; each with its seed)

1. **Unbounded source — biggest lift, cleanest shape.** Execution is already **push-based**, and
   the JS streaming-extract (`records.Recipe.ExtractStream`) already proved a **backpressured
   emit** across a goroutine boundary (unbuffered channel, race-clean; [DESIGN-data.md](DESIGN-data.md)).
   A follow-source just never returns EOF — it parks until the next append/message. Everything
   downstream (broadcast, index, filter, project) is unchanged.
2. **Continuous emit / triggers.** Fused filter+project detectors *already emit per row*, so under
   an unbounded source they are continuous with zero new code. Only windowed/aggregate/standalone
   detectors need emit-on-**watermark** (punctuation) instead of the current emit-at-EOF.
3. **Incremental standing state.** Long-running aggregates need incremental update, not batch
   re-fold. n1k1 already did the **O(N) incremental window fold** and the **invertible COUNT
   slide**; generalize that and hang the accumulator on the **spill-backed rhmap** (which already
   survives working sets > RAM), with retention/TTL to bound it.
4. **Event-time + watermarks + out-of-order.** The ASOF merge already assumes a sortable time key
   ([DESIGN-sorting.md](DESIGN-sorting.md) proposes a shared sorted-stream substrate); a watermark
   model extends it to bound how long to wait for late events.
5. **Retention / bounded state** for genuinely unbounded queries (spill helps; needs a TTL story).
6. **Fault tolerance.** Deliberately *shallow*: at-least-once with **restart-from-source-offset**,
   not distributed exactly-once. See Positioning — this is where the heavyweights win and n1k1
   should not compete.

⚠ **The `_meta.pos`-per-file gotcha becomes a `_meta.offset`-per-partition gotcha.** The context
(`grep -A/-B/-C`) window idiom partitions by `_meta.`path`` because positions restart per file
([DESIGN-prepare.md](DESIGN-prepare.md)); the streaming analog is partitioning window state by
source/partition key so state and ordering don't leak across event partitions.

## The agentic angle — "Jarvis for your business"

PREPARE++ already assumes the corpus is *"authored by an AI agent and run by support teams."*
Generalize from support bundles to live business data and the agentic-monitoring loop falls out:

> An LLM turns *"tell me when a tier-gold customer shows a churn signal within 24h of a payment
> failure"* → a SQL++ detector committed to the **git corpus** → n1k1 runs it **continuously** over
> the CDC / event stream → a findings row → an alert or action.

The differentiator is the **feedback loop the AI author gets**, which almost no CEP/rule system
offers: `.multi lint` already reports, per detector, *does it fuse? is it index-pruned or
always-wake? native byte path or boxed? does it match its golden fixture?* — a cheap **correctness
+ efficiency oracle** an agent can iterate against. That is what makes an LLM-authored rule corpus
*trustworthy and fast* rather than a pile of unvetted queries, and it would be a moat: competitors
would have to build the oracle, not just accept the rules.

Two natural first beachheads — the user's own two examples:
- **CDC monitoring.** Debezium → (Kafka/topic source) → n1k1 corpus → findings. n1k1 already reads
  the on-ramp shapes (JSON/jsonl(.gz)/CSV/Parquet); a blocking Kafka/CDC source closes it.
- **Dataset monitoring.** Freshness/volume/schema-drift/distribution monitors *are* detectors —
  `COUNT(*) per window < threshold`, a schema-change predicate, a distribution-shift check — so
  **the corpus IS the monitor library** (the Monte-Carlo/Anomalo job, but embeddable and in SQL).

## Positioning — the open niche: "the DuckDB of CEP"

Every established player is either a **distributed cluster** you provision (Flink, Spark
Structured Streaming, Materialize, RisingWave) or a **domain-locked rule language** (Sigma/SPL/KQL,
security only). The gap n1k1 could own:

> **A single pure-Go, CGO-free, cross-compiled binary** running a **git corpus of plain-SQL++
> standing rules** with **MQO scale** (the AhoCorasick predicate index), **authored + maintained by
> an AI agent**, deployable **embedded / at the edge / inside the bundle** with **zero
> infrastructure**.

The wedge is the *combination*, not any single axis: **embedded + SQL++ + AI-authored git corpus +
MQO**. DuckDB proved "serious SQL, no cluster, one binary" is a huge market for OLAP; the CEP
equivalent (standing detection rules, no cluster, one binary, AI-authored) is comparatively empty.

## Landscape (as of 2026) — who plays here

Four adjacent clusters, all converging on "standing SQL over a change stream, increasingly
AI-flavored":

- **Stream processors (cluster-scale).** Apache **Flink** (incumbent; pushed hard as **Flink SQL by
  Confluent** post-Immerok), **Spark Structured Streaming** (Databricks), **Kafka Streams / ksqlDB**,
  and **Arroyo** (Rust, SQL) — **acquired by Cloudflare (2025)**, now powering Cloudflare Pipelines.
- **Streaming databases / IVM (closest conceptual match).** **Materialize** (differential dataflow,
  Postgres-wire), **RisingWave** (Rust, Postgres-wire, OSS) — now explicitly branding itself
  *"Streaming Infrastructure for Agentic AI,"* **Feldera** (DBSP incremental theory), **Timeplus /
  Proton** (ClickHouse-based; markets *"CEP made easy with streaming SQL + UDF"* — the nearest pitch
  to n1k1's), **Epsio** / **Readyset** (IVM bolted onto existing Postgres/MySQL via CDC), plus
  warehouse-native incremental (Snowflake Dynamic Tables, Databricks DLT).
- **Classic CEP (StreamBase heritage).** TIBCO **Streaming** (the StreamBase lineage), **Esper**
  (OSS EPL), Flink CEP.
- **Detections-as-code / SIEM (the "corpus of detectors" domain).** **Sigma** (vendor-neutral OSS
  detection-rule format — spiritually identical to n1k1's SQL++ corpus, different rule language),
  **Panther** (detections-as-code, streaming), **Elastic Security** (ES|QL rules), **Splunk** (SPL),
  **Falco** (runtime rules). Real 2025 convergence: Rust "Flink-alternatives for detection
  engineering" pairing Sigma + MITRE ATT&CK with stream processing.
- **Data observability + agentic (monitoring *datasets*).** **Monte Carlo** and **Anomalo**
  (freshness/volume/schema-drift/distribution) — *both added "agentic observability" layers in 2025*
  trying to bridge detection→resolution; Bigeye / Metaplane nearby.
- **AI-SRE agents (frothy, 2025–26).** A wave of autonomous monitor-and-diagnose agents (OpenObserve,
  IBM Instana GenAI, and many startups). Consistent analyst read: many single-purpose AI-SRE
  startups now, **consolidating** toward "one platform orchestrating many agents" by ~2027.
- **CDC on-ramp.** **Debezium** (OSS standard), **Estuary Flow**, **Decodable** (managed Flink),
  **Striim**.

Sources: [RisingWave — streaming DB landscape 2026](https://risingwave.com/blog/streaming-database-landscape-2026-complete-guide/) ·
[RisingWave home ("Streaming Infra for Agentic AI")](https://risingwave.com/) ·
[Epsio / IVM / differential dataflow](https://materializedview.io/p/epsio-ivms-differential-dataflow) ·
[Materialize vs RisingWave](https://materialize.com/guides/materialize-vs-risingwave/) ·
[Timeplus — CEP with streaming SQL + UDF](https://www.timeplus.com/post/cep-with-streaming-sql-udf) ·
[detection-as-code (GitHub topic)](https://github.com/topics/detection-as-code) ·
[Sigma — Complex Event Processing](https://www.sigmacomputing.com/blog/complex-event-processing-cep) ·
[Monte Carlo vs Anomalo](https://www.anomalo.com/blog/monte-carlo-vs-anomalo/) ·
[Estuary — CDC reality](https://estuary.dev/blog/cdc-magic/) ·
[Mezmo — 5 startups defining AI SRE](https://www.mezmo.com/newsroom/5-startups-defining-ai-sre) ·
[Fabrix.ai — 2026: agentic AI disrupts observability](https://fabrix.ai/blog/2026-the-year-agentic-ai-disrupts-observability-security-and-enterprise-saas/)

## Risks & open questions

- **Exactly-once + distributed fault tolerance is the heavyweights' moat.** Don't chase it; own the
  embedded / at-least-once / restart-from-offset / edge lane. Be explicit that n1k1 is not Flink.
- **Continuous operation is real engine work** (weeks, not days) even with good seeds — the
  unbounded source + watermark/emit model + incremental-state generalization are each non-trivial.
- **Crowded, consolidating market.** The wedge must be the combination nobody else has (embedded +
  SQL++ + AI-authored git corpus + MQO), not a me-too stream processor.
- **Grammar constraint still holds** ([DESIGN-prepare.md](DESIGN-prepare.md)): no dialect changes —
  windows/rate/burst/streak/gap via stock `OVER (…)`, temporal via the ASOF idiom, no `EMIT`/`STREAM`
  keywords. Continuous semantics must ride config/pragmas/CLI, not new SQL++ syntax. A `.macro.js`
  could sugar common CEP patterns (rate/burst/absence) into stock SQL++ without touching the parser.
- **State durability model unsettled** — where does long-running aggregate/window state live across
  restarts (spill files? an offset+snapshot?), and how is it reconciled with source replay.
- **Delta-report synergy** — the PREPARE++ re-run **delta report** (keyed by fingerprint + corpus
  SHA) is the batch analog of "emit only on change"; the two should share a design.
