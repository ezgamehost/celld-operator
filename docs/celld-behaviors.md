# celld behaviors this operator encodes

The reconciler's guardrails and the rollout state machine exist because of
specific, verifiable celld behaviors. This file is the index the code
comments cite (F1–F14). Source of truth: celld's docs and source
(verified against v0.3.0); if upstream changes a row, find its citations
in this repo and revisit them.

| # | celld behavior | What this operator does about it |
| --- | --- | --- |
| F1 | Two listeners: public (`--listen`, serves the Worker, reserves only `/__celld/health`) and internal (`--internal-listen`, peer protocol + operator API). The operator API is **unauthenticated** except `/__d1/` (fleet-secret HMAC, see the wire notes). | Separate Services; the internal listener gets NetworkPolicy + (where Istio enforces) AuthorizationPolicy, and is never routed. |
| F2 | Peers dial each other at the `--advertise` address; peer requests carry an HMAC, body signature, clock limit and replay protection, but celld terminates no TLS; docs require a trusted network or encrypted overlay. | Stable per-pod DNS via the headless Service; encryption is the cluster's job (mesh or CNI). |
| F3 | One fleet runs one application deployment. | Fleet-per-app tenancy; one WorkerApp = one fleet = one bucket prefix with its own credentials, so a full runtime compromise reaches only that app. |
| F4 | Nodes load the app deployment from the bucket's `deploy/current.json` **at startup only**. The manifest names the features it needs (`assets-v1`, `wasm-v1`; since v0.3.0 also `cron-v1`, `d1-v1`, `sqlite-vec-v1`) and a node that predates one rejects the deployment up front. | Deploys are bucket-publish **plus rolling restart**; the pod-template app-version annotation is the rollout trigger, and `appVersion: auto` follows the pointer directly. A deployment that needs a feature the running celld lacks fails the first released pod's readiness and holds the rollout there — bump `celld.image` before deploying such an app. |
| F5 | Rolling-update rule: after restarting a node, wait for **every** node to report `restoring=0` before the next restart — the restore work lands on the *peers*, not the replacement pod. `restoring` counts each cold route holding or awaiting an activation permit (v0.3.0 also reports its parts: `activating`, `activation_waiting`, `capacity_waiting`). | The operator owns the StatefulSet partition and steps it one ordinal at a time, gating on a live fleet-wide `/state` sweep. |
| F6 | SIGTERM starts a graceful drain: health flips to 503, new requests get 503+close, cells hand off to peers (at most `CELLD_RELEASES`=128 at once), bounded by `CELLD_SHUTDOWN_DRAIN_MS` (default 25000); an idle node exits at once. After the drain the node seals its node log with one bucket CAS (`node-log close: sealed epoch`), which is what makes a later downgrade safe (F8). | Termination grace (40 s) above the drain bound leaves the seal its headroom; 503 retries at the edge; readiness on the health path; **no HTTP liveness probe on it** (it would kill draining nodes). |
| F7 | The bucket must provide conditional create (`If-None-Match: *`), conditional overwrite (`If-Match`/ETag CAS), and read-after-write consistency, enforced atomically. celld speaks the S3, GCS and (since v0.3.0) Azure Blob dialects (`bucket.rs`). Qualified: Amazon S3, Cloudflare R2, Google Cloud Storage, Tigris, Azure Blob Storage; not: MinIO CE, Backblaze B2, Hetzner, DigitalOcean Spaces, Azurite. An `az://` name is a *container*; the account comes from `AZURE_STORAGE_ACCOUNT_NAME` and exactly one credential family (account key, VM/AKS-node managed identity via IMDS, or AKS workload identity); `--endpoint` is rejected for `gs://` and `az://`. Bucket credentials are fleet-admin authority. | Qualified-store guidance in the README; `celld diagnose` + `hack/cas-hammer` for anything else (F14); per-fleet credential scoping. `spec.bucket.storageAccount` renders `AZURE_STORAGE_ACCOUNT_NAME`, `credentialsFrom.azureClientID` wires AKS workload identity, `secretRef` injects an account key; the CRD refuses `az://` without an account and an endpoint on `gs://`/`az://`. |
| F8 | Some celld upgrades are explicitly **not rolling-safe** (v0.1 ↔ v0.2: mixed fleets break). v0.2.1 → v0.3.0 **is** rolling-safe (a v0.3 node that cannot replicate to a v0.2 peer falls back to bucket proofs until the peer upgrades), but the **downgrade** v0.3 → v0.2 can lose acknowledged writes unless every node's shutdown log shows `node-log close: sealed epoch`. Security fixes land on the latest release only. | The `breakingBoundaries` table is directional and checks every boundary a jump crosses; Rolling across a flagged crossing is refused with the upstream reason unless the CR says `Recreate`, which also surfaces the reason while the fleet stops. Support policy is latest-only. |
| F9 | Telemetry (`CELLD_OTEL=1`) writes Parquet traces/logs to the fleet bucket or OTLP; W3C `traceparent` propagates both ways. celld exports no metrics endpoint. | Telemetry env wired from `spec.telemetry`; the operator polls `/state` and re-exports Prometheus metrics itself. |
| F10 | No rebalancing on node join; placement is traffic-driven. Pressure shedding has two ceilings (`logic/pressure.rs` `PressureConfig::from_limits`): the threshold `CELLD_MAX_RSS_MB` (default 80% of what the process may use, which celld reads from the cgroup limit in `machine.rs` `total_memory_bytes` before `/proc/meminfo`) applies to the memory the **cells hold** (`in_use_bytes` = RSS minus allocator retention, the only memory shedding can return); an absolute cap at 95% of the limit applies to the process **RSS**. Each latch releases at 80% of its own ceiling; `/state` reports the reason (`memory` or `rss-hard`) and both numbers; `0` disables both. | `CELLD_MAX_RSS_MB` is set to 80% of the container limit — under the cap, so shedding keeps its recovery property, and visible in the pod spec. `celld_rss_bytes` and `celld_in_use_bytes` are exported; autoscaling scales up early (new capacity absorbs slowly) and treats shedding as the hard out-of-capacity trigger. |
| F11 | `celld deploy` = esbuild + `wrangler.jsonc`/`.json` with a strict key allowlist (v0.3.0 adds `triggers.crons` and `d1_databases`); unknown keys fail loudly. `--dry-run` bundles without writing. | CI-validated deploys; the operator never touches the build. |
| F12 | Self-fencing: a node whose lease lapses, or whose core actor or replication process dies, logs a `SELF-FENCE:` line and exits with code **3**. The state is terminal, and upstream **requires** a supervisor that restarts the process without an attempt limit and waits at least one lease lifetime (`CELLD_TTL_MS`, default 10 s) between attempts. | Kubernetes restarts the container without a limit, but CrashLoopBackOff is sufficient only when the effective kubelet delay before each retry is at least `CELLD_TTL_MS`. The default 10 s initial delay covers the default lease lifetime; a larger `CELLD_TTL_MS` requires a correspondingly longer kubelet backoff or an explicit supervisor delay. The operator exports `celld_container_restarts` and `celld_self_fenced` (last exit code 3) so a fence loop is visible rather than silent. |
| F13 | Fleet durability (`CELLD_DURABILITY`, default `fleet` since v0.3.0; `bucket` is the pre-0.3 behavior): a write is acknowledged when **two follower nodes** hold it on local disk (write-all, ack-all) or the bucket upload proves it, whichever first; the bucket remains the tiering target. A follower failure degrades the leader to bucket proofs until it re-recruits; a takeover of a dead leader recovers from at least one reachable sealed follower before it restores. A one-node fleet behaves as `bucket`. | `spec.durability` pins the proof; a soft hostname topology spread keeps a leader and its followers off one host, since until the upload lands an acknowledged write exists only on their emptyDirs; the per-pod `CELLD_WATCH` emptyDir is otherwise unchanged (its loss is the node-failure case upstream designs for). |
| F14 | Startup storage probe (`CELLD_STORAGE_PROBE`, default on): every node writes and deletes one small object under `probe/` to test the four conditional-write transitions (create, reject-create, update, reject-stale) and stops if the store fails any — a store that ignores the precondition would otherwise make the node self-fence in a loop. `celld diagnose` runs the same test (`--read-only` skips it). celld reserves `probe/`, `cells/`, `nodes/`, `node-cells/`, `fleet/`, `deploy/`, `deploy-blobs/`, `wake/` and `telemetry/` under the prefix. | Per-fleet credentials must allow put and delete under the prefix (they already must for cells); README qualification guidance is `celld diagnose` for the sequential contract and `hack/cas-hammer` for atomicity under racers (S3 API only). |

## Wire-format notes (learned from source, not docs)

- `/state` (`actor.rs` `state_json`) returns
  `{"ownership","occupied","evicting","restoring","activating","activation_waiting","capacity_waiting","phases","shedding","rss_bytes","in_use_bytes","residents","published","publishes","stops"}`
  where `shedding` is `null` or a **reason string** (`"memory"` for the
  cell-memory threshold, `"rss-hard"` for the absolute RSS cap), not a
  boolean, and `restoring` is the activation backlog. The three breakdown
  counters and both memory numbers are new in v0.3.0 (absent on older
  nodes, so they decode as zero).
- `deploy/current.json` is `{script_name?, version, prefix, rollout}`;
  `version` is the ID `celld deploy` prints. The manifest it points at
  carries a `features` list (F4).
- `CELLD_VARS_FILE` is env-file format (`NAME=value` lines, `#` comments,
  optional quotes), hence the `vars.env` Secret key convention.
- `CELLD_IDLE_EVICT_S` is **disabled unless set**: idle cells stay resident
  indefinitely by default.
- Boolean celld variables accept only `0` or `1`; a supplied invalid value
  stops startup, so the operator writes `1`, never `true`.
- The internal listener also serves `/cell/` (resolve *or activate*),
  `/evict/`, `/do/` (send a direct Durable Object request; refuses a D1
  database), `/__d1/SCOPE` (SQL to a D1 database, authenticated with the
  fleet secret from the bucket — what `celld d1` uses), `POST /shutdown`,
  `POST /shutdown?handoff=preserve`, and `/__celld/probe` — unused by the
  operator today, available for ops tooling. Note `/cell/` and `/do/` mean
  an unauthenticated caller who reaches the port can execute application
  code and mutate cell state, not merely inspect it; only the D1 route
  authenticates.
- Cron triggers (`triggers.crons`) run as one reserved cell per script
  (`.cron:<script>`), so ownership CAS fires each schedule once per fleet,
  not once per node. Nothing for the operator to schedule; a fleet scaled
  to zero fires nothing.

## Version pinning

celld's operator API (`/state` et al.) is alpha and can change between
releases; pin the operator and celld releases together, and treat every
operator upgrade note about `breakingBoundaries` as part of upgrading celld.

## Known not-implemented

`credentialsFrom.iamRole: auto` (operator-provisioned IAM) is surfaced as a
condition and left to the cluster operator. `iamRole` renders only the EKS
IRSA annotation, so GKE Workload Identity is not wired; GCS credential wiring
is feasible (celld uses ADC via `GoogleCloudStorageBuilder::from_env`) but not
built yet. Azure is wired for an account key (`secretRef`) and AKS workload
identity (`azureClientID`); a node-level managed identity needs only
`storageAccount`, since celld reads it from IMDS. Deploy tracking
(`appVersion: auto`) reads `s3://` buckets only, so a `gs://` or `az://`
fleet must pin `appVersion` (reported as `DeployTrackingReady:
UnsupportedStore`). `WorkerApp` exposes no pod scheduling fields
(nodeSelector, affinity, tolerations) beyond the built-in hostname spread:
node-pool placement needs a cluster-level mechanism.
