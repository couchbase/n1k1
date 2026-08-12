# Ideas: Infrastructure-state use cases (k8s, Terraform, multi-cloud)

_Brainstorm, 2026-08-12. **Exploratory — not a commitment.** No code implied; this records
which queries would be compelling over infrastructure-state data, what people use today
instead, and where n1k1 would (and would NOT) bring something new. Companion to
`DESIGN-data.md` §9 (the etcd dump recipes) and `DESIGN-merging.md` (ASOF)._

The prompt: pretend we have etcd/apiserver data for a realistic k8s estate —
prod/test/backup clusters across multiple clouds — plus the usual observability exhaust.
What's possible, what's *interesting*, and what's genuinely new?

## Data-shape reality check (this constrains everything below)

- **k8s objects come from the apiserver, not raw etcd** — raw etcd values are protobuf
  (`DESIGN-data.md` §9's protobuf box). `kubectl get -o json` is the practical source.
- ⚠ **etcd compacts.** Its MVCC history window is typically minutes-to-hours, so
  time-travel against a *live* etcd only reaches the compaction horizon. Durable history
  comes from **snapshots you keep** — §9's Recipe B (`rev-<N>.jsonl`), Velero backups, or
  periodic `kubectl get -o json` dumps. This is why the snapshot-dump design is the right
  one, not a workaround.
- So the realistic corpus is: **a directory of point-in-time snapshots × N clusters ×
  M clouds**, plus metrics/log/billing exports — i.e. exactly the multi-source federation
  (`DESIGN-data.md` §2) n1k1 already does.

## Query classes

### 1. Posture / inventory — the "kubectl can't join" class

```sql
-- containers with no memory limit, by cluster + owning team
SELECT p._cluster, p.metadata.labels.team, COUNT(*) AS n
FROM pods p UNNEST p.spec.containers AS c
WHERE c.resources.limits.memory IS MISSING
GROUP BY p._cluster, p.metadata.labels.team ORDER BY n DESC;
```

The rich genre here is **reference integrity**, which is all anti-joins: Pods referencing
Secrets/ConfigMaps that don't exist; Secrets nothing references (orphan + risk); PVCs bound
to a StorageClass absent in the DR cluster (a failover blocker found *before* the failover).
**RBAC** is the standout: `ServiceAccount → RoleBinding → Role → rules` is a 3-hop join +
`UNNEST`, so "who can transitively `create pods/exec`" — genuinely hard with kubectl — is an
ordinary query.

### 2. Multi-cluster diff — "why does it work in test?"

```sql
-- same Deployment name, different image between prod and test
SELECT pr.metadata.name AS name, pr._image AS prod_image, te._image AS test_image
FROM prod_deploys pr JOIN test_deploys te ON pr.metadata.name = te.metadata.name
WHERE pr._image != te._image;
```

⚠ **Gap this use case exposes:** "in prod but NOT in test" wants `FULL OUTER JOIN`, which
n1k1 lacks (cbq-fork parser gap + a new engine op — see TODO/`joins-advanced`). The
workaround is `UNION ALL` + `GROUP BY name HAVING COUNT(*) = 1`. If infra-state becomes a
real target, FULL OUTER moves up the priority list.

### 3. Time travel — where it starts feeling like a superpower

Diffing two snapshots is already a shipped shape (`.multi cursor create --mode diff` →
Debezium-style insert/update/delete envelopes):

- **"What changed in the 40 minutes around the incident?"** grouped by kind/namespace — a
  far better first question than tailing logs.
- **Flapping/fighting controllers**: `HAVING COUNT(DISTINCT revision) > 10` for one object
  in an hour reveals two controllers reconciling against each other — a real, nasty,
  hard-to-see pathology.
- **Reconstruct the deploy timeline from state**, not from CI — including hand-edits CI
  never saw.
- **GitOps drift**: join the snapshot against the *git working tree of manifests* (n1k1
  reads those YAMLs as an ordinary keyspace) → what was `kubectl edit`-ed and never
  committed. Drift detection with no controller and no cluster access.

### 4. Cross-source joins — the strongest differentiator

n1k1 already has watermarked near-sorted merge + **ASOF** temporal correlation
(`DESIGN-merging.md`), which is exactly the shape of "what was true when this happened."
⚠ There is **no `ASOF JOIN` keyword** (the cbq grammar is off-limits); ASOF is *recognized*
from the correlated-argmax subquery form:

```sql
-- for each config change, the CPU sample in effect at that moment
SELECT ch.name, ch.rev,
       (SELECT m.cpu FROM metrics m
        WHERE m.ts <= ch.ts AND m.pod = ch.name
        ORDER BY m.ts DESC LIMIT 1) AS cpu_at_change
FROM changes ch;
```

Combinations worth chasing: state-change timeline ⋈ Prometheus/Datadog export ⋈
Loki/Logstash lines ⋈ **AWS CUR Parquet in S3** (cost per namespace/team without a vendor).
All offline, one engine. Datadog cannot join your cluster spec; your warehouse does not
have the etcd snapshots.

## Honest scorecard — where we'd be new, and where we'd be late

| Need | Today's best tool | New? |
|---|---|---|
| SQL over **live** cloud/k8s inventory | **Steampipe** (Postgres FDW, 140+ plugins, cross-cloud joins); Powerpipe benchmarks | **No** — mature, owns this |
| Sync cloud → warehouse, then SQL | **CloudQuery** | No |
| Per-cloud inventory SQL | AWS Config advanced queries; Azure Resource Graph (KQL); GCP Cloud Asset Inventory → BigQuery (**has ~35-day asset history**) | No |
| Policy/guardrails on k8s objects | OPA/Gatekeeper, Kyverno | No — and they're better (admission-time enforcement) |
| Metrics / time series | Prometheus, Datadog | No, not close |
| "Who changed what" | k8s audit logs → Elastic/Splunk/Loki | No |
| **Offline/air-gapped forensics over a dump someone emailed you** | jq + despair | **Yes** |
| **Diffing two points in time as a first-class op** | bespoke scripts | **Mostly yes** |
| **ASOF-correlating state ⋈ metrics ⋈ logs in ONE engine** | stitch 3 tools + a human | **Yes — rare** |
| **A versioned corpus of N hundred detectors over ONE shared scan** | Powerpipe benchmarks (per-control queries) | **Yes — mechanically different** (MQO) |

**Positioning that follows:** n1k1 is *not* "a better Steampipe for live inspection" — it is
the **offline time machine**. The cbcollect_info heritage (`DESIGN-prepare.md`) maps onto
k8s almost exactly: a customer sends a snapshot bundle, you can't reach their cluster, and
you run a **git-versioned detector pack** over it in one shared scan.

## Non-k8s stories

- **Terraform — arguably the better FIRST target than k8s.** The data is already clean JSON
  (`terraform show -json`, plan JSON): no protobuf hop, no apiserver. The pain is real —
  50 repos × 50 state files and nobody can answer "where is this resource actually
  managed?" Plan-diff as a pre-merge gate ("what does this apply *destroy*?") is a natural
  query. Alternatives exist (Steampipe's terraform plugin, Conftest/OPA on plans), but
  **multi-state federation + plan-vs-plan diffing** is thin.
- **AWS / GCP / Azure.** Per-cloud SQL already exists (table above). Our angle is the same
  as k8s: offline snapshots, cross-vendor in one query, and cost joins against CUR Parquet.

## The "$ANY_CLOUD → k8s view" question

**Verdict: the thin version is real and valuable; the deep version is a categorical error.**
The boundary is crisp, and worth stating so nobody builds the wrong half.

It is not a crazy idea — the industry already did it: **Crossplane**, **AWS ACK**, **Azure
Service Operator**, **GCP Config Connector** all project cloud resources as k8s CRDs with
`spec`/`status`, and AWS Config's "configuration item" (type + id + tags + relationships) is
already k8s-envelope-shaped.

**Where it holds — the envelope, not the ontology.** Every provider has identity (ARN /
self-link / uid), a type, tags ≈ labels, a container (account/project/subscription), a
parent, timestamps, and a payload. A per-provider SQL++ VIEW projecting into:

```
{ provider, kind, id, account, region, labels, parent, created, raw }
```

enables the real cross-cloud questions ("everything tagged `team=payments` across AWS + GCP
+ k8s, with cost"). n1k1 is unusually suited because it's **schemaless**: keep `raw` as the
native nested doc and query provider-specific fields directly. **You normalize the join
keys, not the ontology.** Natural home: the catalog-defined VIEW proposal (`DESIGN-data.md`
§2), with `*.macro.js` generating the per-provider projections.

**Where it breaks (apples-to-Wednesdays):**
- **`spec` vs `status` doesn't transfer.** In k8s, `spec` is *desired* state under
  continuous reconciliation; raw AWS has no desired state unless something (Terraform,
  Crossplane) owns it. So "spec ≠ status ⇒ drift" — the query that makes k8s special — is
  meaningless on a bare EC2 instance. You effectively only have `status`.
- **Namespace ≠ account.** A namespace is a soft in-cluster grouping (RBAC + quota); an
  account/subscription is a hard security and billing boundary. Equating them silently lies
  in exactly the security queries where truth matters.
- **No reconciliation loop**, and different GC semantics (cascade delete vs dependency
  error).
- **Kind mapping turns to mush.** ASG ≈ Deployment is defensible (desired count, replicas).
  Lambda ≈ Job? Weak. S3 bucket ≈ PVC? No.

The failure mode is **semantic false-friends**: a view that *looks* uniform and quietly
answers wrong. Build the thin envelope; refuse the deep ontology mapping.

## If we chase this

1. **Terraform state/plan** first — cleanest data, real pain, no protobuf.
2. **k8s snapshot diffing** — reuses `.multi cursor create --mode diff` + the §9 recipes.
3. **The thin cross-cloud envelope** — as catalog VIEWs, join keys only.

The feature that would make the whole story sing is already half-built: a **detector pack**
(`.multi --queries`) shipped as a git repo of `*.sql++` cluster-conformance checks, run over
any snapshot bundle in **one shared scan**.

**Prerequisites this story would surface:** `FULL OUTER JOIN` (multi-cluster set diffs),
and — if raw etcd rather than the apiserver is ever the source — the protobuf wall (§9).
