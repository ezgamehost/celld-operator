# celld behaviors this operator encodes

The reconciler's guardrails and the rollout state machine exist because of
specific, verifiable celld behaviors. This file is the index the code
comments cite (F1–F14). Source of truth: celld's docs and source
(verified against v0.4.0); if upstream changes a row, find its citations
in this repo and revisit them.

| # | celld behavior | What this operator does about it |
| --- | --- | --- |
| F1 | Two listeners: public (`--listen`, serves the Worker, reserves only `/.well-known/celld/health`) and internal (`--internal-listen`, versioned peer protocol + alpha operator API). The operator API is unauthenticated except its command-specific checks. | Separate Services; the internal listener gets NetworkPolicy + (where Istio enforces) AuthorizationPolicy, and is never routed. |
| F2 | Peers dial each other at the `--advertise` address; peer requests carry an HMAC, body signature, clock limit and replay protection, but celld terminates no TLS; docs require a trusted network or encrypted overlay. | Stable per-pod DNS via the headless Service; encryption is the cluster's job (mesh or CNI). |
| F3 | One fleet runs one application deployment. | Fleet-per-app tenancy; one WorkerApp = one fleet = one bucket prefix with its own credentials, so a full runtime compromise reaches only that app. |
| F4 | v0.4 reads `deploy/current.json` at startup, polls it every 30 seconds (`CELLD_DEPLOY_POLL_S`), and adopts a new deployment in place; `POST /reload` adopts immediately. Existing requests finish on the old generation and resident Durable Objects move at safe points. The manifest names the runtime features it needs. | `appVersion` remains the CR's expected deployment and `auto` follows the pointer. The pod-template annotation retains a conservative gated restart as an explicit convergence fence, although v0.4 normally adopts before it. Upgrade `celld.image` before publishing an app that requires new runtime features. |
| F5 | For runtime rollouts, v0.4 paces handoff inside the stopping node and gates a replacement's first healthy response on fleet capacity, pressure, and restore backlog. `/state.restoring` remains the activation backlog (`activating` + `activation_waiting` + `capacity_waiting`). | The operator still owns the StatefulSet partition and steps one ordinal at a time, conservatively requiring a live fleet-wide `/state` sweep with `restoring=0` in addition to the replacement's readiness gate. |
| F6 | SIGTERM starts a graceful drain: health flips to 503, new requests get 503+close, and cells hand off in batches (`CELLD_RELEASES`, default 8). `CELLD_DRAIN_TOKEN_WAIT_MS` (default 30000) serializes simultaneous donors; `CELLD_SHUTDOWN_DRAIN_MS` (default 25000) is the no-progress interval; `CELLD_SHUTDOWN_TOTAL_MS` (default 40000) bounds the complete stop. After handoff the node seals its log. | Pin the 40 s celld total bound and set Kubernetes termination grace to 60 s; readiness uses the health path; liveness is TCP because an HTTP probe would kill a draining node. Recreate scales to zero only after every old pod exits. |
| F7 | The bucket must provide conditional create (`If-None-Match: *`), conditional overwrite (`If-Match`/ETag CAS), and read-after-write consistency, enforced atomically. celld speaks the S3, GCS and (since v0.3.0) Azure Blob dialects (`bucket.rs`). Qualified: Amazon S3, Cloudflare R2, Google Cloud Storage, Tigris, Azure Blob Storage; not: MinIO CE, Backblaze B2, Hetzner, DigitalOcean Spaces, Azurite. An `az://` name is a *container*; the account comes from `AZURE_STORAGE_ACCOUNT_NAME` and exactly one credential family (account key, VM/AKS-node managed identity via IMDS, or AKS workload identity); `--endpoint` is rejected for `gs://` and `az://`. Bucket credentials are fleet-admin authority. | Qualified-store guidance in the README; `celld diagnose` + `hack/cas-hammer` for anything else (F14); per-fleet credential scoping. `spec.bucket.storageAccount` renders `AZURE_STORAGE_ACCOUNT_NAME`, `credentialsFrom.azureClientID` wires AKS workload identity, `secretRef` injects an account key; the CRD refuses `az://` without an account and an endpoint on `gs://`/`az://`. |
| F8 | v0.1 ↔ v0.2 cannot mix. v0.2.1 → v0.3.0 is rolling-safe, but v0.3 → v0.2 can lose acknowledged writes unless each v0.3 node sealed its log. v0.3 ↔ v0.4 cannot mix: the v0.4 peer tunnel refuses v0.3 and v0.3 cannot read v0.4 epoch-qualified large Workers KV references. Security fixes land on the latest release only. | `breakingBoundaries` checks every crossed boundary and direction. Rolling across a known hazard is refused with the upstream reason unless the CR says `Recreate`; v0.3 ↔ v0.4 therefore stops every old node before any new one starts. |
| F9 | Telemetry (`CELLD_OTEL=1`) writes Parquet traces/logs to the fleet bucket or OTLP; W3C `traceparent` propagates both ways. celld exports no metrics endpoint. | Telemetry env wired from `spec.telemetry`; the operator polls `/state` and re-exports Prometheus metrics itself. |
| F10 | No rebalancing on node join; placement is traffic-driven. v0.4's pressure threshold (`CELLD_MAX_RSS_MB`, default 80% of available memory) uses the greater of allocator-adjusted RSS (`in_use_bytes`) and active cgroup memory (`cgroup_working_set_bytes`). The absolute 95% cap uses complete cgroup charge (`cgroup_current_bytes`), falling back to process RSS when cgroup data is unavailable. `0` disables both. | Set `CELLD_MAX_RSS_MB` to 80% of the container limit. Export all four memory measurements; autoscaling scales up early and treats shedding as the hard out-of-capacity trigger. |
| F11 | `celld deploy` = esbuild + `wrangler.jsonc`/`.json` with a strict key allowlist; v0.4 adds Workers KV, Queues, Workflows, R2 bindings, and broader compatibility settings. Unknown keys fail loudly; `--dry-run` bundles without writing. | CI-validated deploys; the operator never touches the build. |
| F12 | Self-fencing: a node whose lease lapses, or whose core actor or replication process dies, logs a `SELF-FENCE:` line and exits with code **3**. The state is terminal, and upstream **requires** a supervisor that restarts the process without an attempt limit and waits at least one lease lifetime (`CELLD_TTL_MS`, default 10 s) between attempts. | Kubernetes restarts the container without a limit, but CrashLoopBackOff is sufficient only when the effective kubelet delay before each retry is at least `CELLD_TTL_MS`. The default 10 s initial delay covers the default lease lifetime; a larger `CELLD_TTL_MS` requires a correspondingly longer kubelet backoff or an explicit supervisor delay. The operator exports `celld_container_restarts` and `celld_self_fenced` (last exit code 3) so a fence loop is visible rather than silent. |
| F13 | Fleet durability (`CELLD_DURABILITY`, default `fleet` since v0.3.0; `bucket` is the pre-0.3 behavior): a write is acknowledged when **two follower nodes** hold it on local disk (write-all, ack-all) or the bucket upload proves it, whichever first; the bucket remains the tiering target. A follower failure degrades the leader to bucket proofs until it re-recruits; a takeover of a dead leader recovers from at least one reachable sealed follower before it restores. A one-node fleet behaves as `bucket`. | `spec.durability` pins the proof; a soft hostname topology spread keeps a leader and its followers off one host, since until the upload lands an acknowledged write exists only on their emptyDirs; the per-pod `CELLD_WATCH` emptyDir is otherwise unchanged (its loss is the node-failure case upstream designs for). |
| F14 | Startup storage probe (`CELLD_STORAGE_PROBE`, default on): every node writes and deletes one small object under `probe/` to test the four conditional-write transitions (create, reject-create, update, reject-stale) and stops if the store fails any — a store that ignores the precondition would otherwise make the node self-fence in a loop. `celld diagnose` runs the same test (`--read-only` skips it). celld reserves `probe/`, `cells/`, `nodes/`, `node-cells/`, `fleet/`, `deploy/`, `deploy-blobs/`, `wake/` and `telemetry/` under the prefix. | Per-fleet credentials must allow put and delete under the prefix (they already must for cells); README qualification guidance is `celld diagnose` for the sequential contract and `hack/cas-hammer` for atomicity under racers (S3 API only). |

## Wire-format notes (learned from source, not docs)

- `/state` (`actor.rs` `state_json`) keeps the v0.3 counters and in v0.4
  adds `quiescing`, `releasing`, `adopting`, `handed_off`,
  `handoff_failed`, `cgroup_working_set_bytes`, and
  `cgroup_current_bytes`. It also adds a `deployment` object containing
  the current version and generation, draining generations, swapping count,
  and each resident cell's generation. `shedding` remains `null` or a reason
  string; the cgroup measurements are `null` when unavailable.
- `deploy/current.json` is `{script_name?, version, prefix, rollout}`;
  `version` is the ID `celld deploy` prints. The manifest it points at
  carries a `features` list (F4).
- `CELLD_VARS_FILE` is env-file format (`NAME=value` lines, `#` comments,
  optional quotes), hence the `vars.env` Secret key convention.
- `CELLD_IDLE_EVICT_S` is **disabled unless set**: idle cells stay resident
  indefinitely by default.
- Boolean celld variables accept only `0` or `1`; a supplied invalid value
  stops startup, so the operator writes `1`, never `true`.
- The unauthenticated internal operator routes include `/state`, `/cell/`,
  `/evict/`, `/do/`, `POST /reload`, `POST /shutdown`, and
  `POST /shutdown?handoff=preserve`. `/cell/` and `/do/` can execute
  application code and mutate cell state, not merely inspect it. Reserved
  runtime classes (D1, Workers KV, Queues, Workflows) use the
  HMAC-authenticated `/runtime/<SCOPE>` protocol instead. `/peer/probe` and
  the versioned peer-tunnel paths are internal protocol surfaces; the
  operator does not call them.
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
IRSA annotation. celld v0.4 also reads EKS Pod Identity credentials injected
by the agent; associate the generated ServiceAccount outside the WorkerApp
and leave `credentialsFrom` empty. GKE Workload Identity is not wired. Azure
is wired for an account key (`secretRef`) and AKS workload identity
(`azureClientID`); a node-level managed identity needs only
`storageAccount`, since celld reads it from IMDS. Deploy tracking
(`appVersion: auto`) reads `s3://` buckets only, so a `gs://` or `az://`
fleet must pin `appVersion` (reported as `DeployTrackingReady:
UnsupportedStore`). `WorkerApp` exposes no pod scheduling fields
(nodeSelector, affinity, tolerations) beyond the built-in hostname spread:
node-pool placement needs a cluster-level mechanism.
