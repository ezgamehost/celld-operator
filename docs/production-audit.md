# Production audit and hardening — 2026-09-10

The audit covered the operator, its celld v0.4.0 integration, API, Helm and
Kustomize packaging, CI, and storage qualification helper. The initial cleanup
was PR #4. The subsequent production hardening addresses the remaining source
findings together. No production cluster or application bucket was accessed.

## Changes

| Finding | Implemented protection |
| --- | --- |
| Correlated loss of ephemeral fleet logs | Default to bucket acknowledgement. Fleet durability requires persistent storage and at least three replicas, including the autoscaling minimum. Persistent fleets require different hosts; optional required zone separation is available. PVCs survive scale-down and deletion. |
| KEDA races with rollouts | Request pause and observe KEDA's paused condition and absence of targeting HPAs before mutating an existing fleet. Recreate persists its transition and waits for an uncached census of old pods to be empty. Unsafe pre-v0.3 downgrades are refused even with Recreate. |
| Partial/stale scaling data | Publish immutable metric snapshots atomically, bound concurrent polling and sweep duration, export completeness and timestamp, and gate every scaling query on a complete sample younger than 60 seconds. Empty Prometheus results fail the scaler. Latency queries include destination namespace. |
| Invalid state unlocks rollout | Reject missing required counters, runtime errors, negative counters, oversized responses and trailing JSON. Require every expected ordinal to be owned, ready and reachable. Persist a 10-second settle deadline between partition releases. |
| Excessive workload/credential authority | Use non-root pods, read-only roots, explicit writable volumes, seccomp, dropped capabilities and no automatic service-account token. Restrict runtime images, IAM roles, Azure identities and bucket endpoints through operator allowlists. Support namespace-scoped operation and restrict internal ingress to selected operator pods. |
| Desired version presented as observed | Separate expected version from per-pod observed version, generation, draining and swapping. Report rollout completion only after live convergence. Offer Restart and InPlace deployment policies. |
| Unbounded or wrong-identity tracking | Require an explicit tracking Secret for auto mode, with no ambient credential fallback. Bound requests, pointer size and cache size; include app and credential identity in cache keys, evict on deletion and back off failures. Reject unsupported auto stores. |
| Stale routes, metadata and conditions | Delete obsolete owned routes, track and remove managed annotations, retire obsolete conditions, and check fresh HTTPRoute acceptance/reference resolution plus Gateway programming. Retry configuration is opt-in; detected schema pruning stops repeated writes and reports failure. |
| Weak configuration validation | Add schema/CEL and runtime checks for credentials, names, replica bounds, resource sizes, storage and image tags. Make bucket identity and storage configuration immutable. Add cluster domain, CPU/ephemeral budgets, scheduling and startup probes. Reject invalid CLI modes and intervals. |
| Packaging and supply chain gaps | Synchronize generated chart schemas/RBAC, derive consistent metrics TLS flags, test chart modes, install the actual CI-built image with readiness waiting, isolate e2e clusters, stop logging command arguments containing credentials, pin build tools/images, and add module and container vulnerability scans. Upgrade the remaining affected modules. |

## Upgrade and operational requirements

Read [production operation and migration](production.md) before upgrading.
Admission and security defaults intentionally reject configurations previously
accepted. In particular, existing fleet-durability deployments without PVCs
need an explicit migration, custom images and identities need allowlists, and
auto tracking needs a dedicated read-only Secret.

The operator does not make WorkerApp authors untrusted tenants. They can deploy
workloads and reference Secrets in their namespace. Namespace RBAC, scoped
bucket credentials, storage behavior, encrypted peer networking and CNI policy
enforcement remain platform responsibilities. IPv6 state URLs are supported;
the runtime listeners currently require IPv4-capable pod networking.

## Validation and limits

Unit and envtest regressions cover malformed state, incomplete fleets, persisted
settle gates, KEDA pause acknowledgement and live HPAs, recreate pod disappearance,
observed deployment convergence, credential rotation/cache isolation, resource
ownership, route/annotation cleanup, PVC/security profiles and guarded queries.
Helm rendering tests cover HTTP, self-signed TLS, cert-manager TLS, disabled
metrics and namespace-scoped RBAC.

The dedicated Kind lifecycle suite passed three tests: manager readiness,
authenticated metrics, and WorkerApp admission, image replacement and PVC
persistence. The last test uses a deterministic HTTP runtime fixture; it verifies
operator behavior, not celld log recovery. Its cluster and kubeconfig were removed.

The module vulnerability scan reports no known vulnerabilities in the resolved
module graph. The local Trivy scan of the rebuilt image reports zero HIGH/CRITICAL findings
in its OS and Go binary after upgrading gRPC to v1.83.2; CI repeats that scan.
Race checks passed for the controller and CAS helper. These results do not cover the separately deployed Rust runtime.

Real celld host/zone loss under acknowledged writes, cloud workload identity,
CNI/mesh enforcement, live KEDA/Prometheus outages, CA-verified scraping and
production-scale load still require qualification on the intended platform.
Fake-client pause tests and rendered TLS tests do not substitute for those tests.
