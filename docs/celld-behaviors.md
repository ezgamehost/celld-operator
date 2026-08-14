# celld behaviors this operator encodes

The reconciler's guardrails and the rollout state machine exist because of
specific, verifiable celld behaviors. This file is the index the code
comments cite (F1–F11). Source of truth: celld's docs and source
(verified against v0.2.0); if upstream changes a row, find its citations
in this repo and revisit them.

| # | celld behavior | What this operator does about it |
| --- | --- | --- |
| F1 | Two listeners: public (`--listen`, serves the Worker, reserves only `/__celld/health`) and internal (`--internal-listen`, peer protocol + **unauthenticated** operator API). | Separate Services; the internal listener gets NetworkPolicy + (where Istio enforces) AuthorizationPolicy, and is never routed. |
| F2 | Peers dial each other at the `--advertise` address; celld terminates no TLS; docs require a trusted network or encrypted overlay. | Stable per-pod DNS via the headless Service; encryption is the cluster's job (mesh or CNI). |
| F3 | One fleet runs one application deployment. | Fleet-per-app tenancy; one WorkerApp = one fleet = one bucket prefix with its own credentials, so a full runtime compromise reaches only that app. |
| F4 | Nodes load the app deployment from the bucket's `deploy/current.json` **at startup only**. | Deploys are bucket-publish **plus rolling restart**; the pod-template app-version annotation is the rollout trigger, and `appVersion: auto` follows the pointer directly. |
| F5 | Rolling-update rule: after restarting a node, wait for **every** node to report `restoring=0` before the next restart — the restore work lands on the *peers*, not the replacement pod. | The operator owns the StatefulSet partition and steps it one ordinal at a time, gating on a live fleet-wide `/state` sweep. |
| F6 | SIGTERM starts a graceful drain: health flips to 503, new requests get 503+close, cells hand off to peers, bounded by `CELLD_SHUTDOWN_DRAIN_MS` (default 25000). | Termination grace above the drain bound; 503 retries at the edge; readiness on the health path; **no HTTP liveness probe on it** (it would kill draining nodes). |
| F7 | The bucket must provide conditional create (`If-None-Match: *`), conditional overwrite (`If-Match`/ETag CAS), and read-after-write consistency, enforced atomically. Bucket credentials are fleet-admin authority. | Qualified-store guidance in the README; `hack/cas-hammer` races conditional PUTs to verify atomicity; per-fleet credential scoping. |
| F8 | Some celld upgrades are explicitly **not rolling-safe** (v0.1 ↔ v0.2: mixed fleets break). Security fixes land on the latest release only. | The `breakingBoundaries` table refuses Rolling across flagged pairs unless the CR says `Recreate`; support policy is latest-only. |
| F9 | Telemetry (`CELLD_OTEL=1`) writes Parquet traces/logs to the fleet bucket or OTLP; W3C `traceparent` propagates both ways. celld exports no metrics endpoint. | Telemetry env wired from `spec.telemetry`; the operator polls `/state` and re-exports Prometheus metrics itself. |
| F10 | No rebalancing on node join; placement is traffic-driven. Pressure shedding via `CELLD_MAX_RSS_MB`, whose default derives from "available memory" and is not cgroup-aware. | `CELLD_MAX_RSS_MB` is always set explicitly from the container limit; autoscaling scales up early (new capacity absorbs slowly) and treats shedding as the hard out-of-capacity trigger. |
| F11 | `celld deploy` = esbuild + `wrangler.jsonc`/`.json` with a strict key allowlist; unknown keys fail loudly. `--dry-run` bundles without writing. | CI-validated deploys; the operator never touches the build. |

## Wire-format notes (learned from source, not docs)

- `/state` returns `{"ownership","occupied","evicting","restoring","shedding",...}`
  where `shedding` is `null` or a **reason string** (e.g. `"rss"`), not a
  boolean, and `restoring` is the activation backlog.
- `deploy/current.json` is `{script_name?, version, prefix, rollout}`;
  `version` is the ID `celld deploy` prints.
- `CELLD_VARS_FILE` is env-file format (`NAME=value` lines, `#` comments,
  optional quotes), hence the `vars.env` Secret key convention.
- `CELLD_IDLE_EVICT_S` is **disabled unless set**: idle cells stay resident
  indefinitely by default.
- The internal listener also serves `/cell/`, `/evict/`, `/do/`,
  `POST /shutdown?handoff=preserve`, and `/__celld/probe` — unused by the
  operator today, available for ops tooling.

## Version pinning

celld's operator API (`/state` et al.) is alpha and can change between
releases; pin the operator and celld releases together, and treat every
operator upgrade note about `breakingBoundaries` as part of upgrading celld.

## Known not-implemented

`credentialsFrom.iamRole: auto` (operator-provisioned IAM) is surfaced as a
condition and left to the cluster operator; GCS credential wiring is
feasible (celld uses ADC via `GoogleCloudStorageBuilder::from_env`) but not
built yet.
