# Production audit — 2026-09-10

Scope: operator source at `1905ee7`, CRD/API, Helm and Kustomize packaging,
CI, tests, and the storage qualification helper. The celld integration was
cross-checked against the adjacent celld checkout at tag `v0.4.0`, notably
`actor.rs`, `generation.rs`, `main.rs`, and `docs/guarantees.md`. No production
cluster or application bucket was accessed. This is a source review and test
report, not a certification of storage, CNI, mesh, or runtime behavior.

## Cleanup included

- Upgrade `golang.org/x/text` to v0.39.0 and gRPC to v1.82.1 (with their
  required transitive updates). The initial govulncheck v1.8.0 scan reported
  reachable affected symbols for [GO-2026-5970](https://pkg.go.dev/vuln/GO-2026-5970)
  and [GO-2026-6061](https://pkg.go.dev/vuln/GO-2026-6061). These are static
  call-graph findings; exploitability was not reproduced.

- Reject updates to typed resources, optional integration resources, and
  StatefulSets unless their controller owner UID is the current WorkerApp.
  ScaledObject cleanup also checks ownership and uses UID/resource-version
  delete preconditions. Previously, name collisions could overwrite or delete
  unrelated objects, including resources left by a previous WorkerApp UID.
  Existing installations with unowned children now get an error: inspect and
  resolve the collision rather than having the operator silently adopt it.
- Skip reconciliation of deleting WorkerApps and skip unchanged status writes.
  The latter avoids status events repeatedly waking the primary resource watch.
- Preserve allocated Service node ports and foreign ServiceAccount annotations.
  In `iamRole: auto` mode, preserve the externally provisioned identity promised
  by the existing condition message; explicit identity configuration still wins.
- Watch owned networking.k8s.io Ingress objects, retry failed ScaledObject
  cleanup, distinguish a missing map key from an empty value, handle IPv6
  addresses in the state client, and tolerate nonpositive poll intervals.
  IPv6 URL handling alone does not provide IPv6-only fleet support: listener
  addresses still bind IPv4 and cluster DNS configuration is hardcoded.
- Correct misleading internal comments about deployment polling and settle time.
- Fix `cas-hammer` to read the seed ETag once before releasing writers. Reading
  an ETag independently in each writer permitted valid sequential writes to
  appear as multiple winners. Reject zero rounds or fewer than two writers.

## Feature changes recommended before production

Priorities below describe impact when the stated trigger applies. They are
not claims that every installation will experience the failure.

### P1: Protect acknowledged writes from correlated node loss

[`buildPodTemplate`](../internal/controller/fleet_resources.go) uses `emptyDir`
for local state and `ScheduleAnyway` hostname spread. With fleet durability,
acknowledged writes may still reside only on the owner/follower disks before
bucket upload. Soft placement permits those copies on one host, and there is
no zone constraint. A correlated loss can remove every unuploaded copy.

Add an explicit production storage/scheduling profile: persistent local state,
required host/zone placement with a documented minimum topology, configurable
resources and ephemeral-storage limits, and a tested recovery procedure. For
workloads that need bucket-backed acknowledgement regardless of placement,
use the existing `spec.durability: bucket` and measure the latency tradeoff.
Test host loss and fleet restart while writes are being acknowledged. Upstream's
[durability contract](https://github.com/denoland/celld/blob/v0.4.0/docs/guarantees.md)
should define the acceptance criteria.

### P1: Make KEDA pause an acknowledged step before changing the fleet

[`Reconcile`](../internal/controller/workerapp_controller.go) changes the
StatefulSet before calling `ensureScaledObject`. A pause error therefore occurs
after a template or replica change. Even a successful annotation write does not
prove KEDA/HPA has stopped changing replicas. `recreateStep` also relies on
StatefulSet status replica count to decide that old pods have exited; it does
not list old pods or check status freshness at that transition.

Persist a rollout phase that requests pause, observes KEDA's paused state,
then changes the StatefulSet. Confirm old pods (including terminating pods)
are absent before starting an incompatible runtime. Resume scaling only after
convergence. Cover delayed KEDA reconciliation, API write failures, concurrent
scale-up/down, controller restart, and rapid spec changes in integration tests.
Treat lease/durability shutdown verification for lossy downgrades separately:
`Recreate` by itself does not prove every old node successfully sealed its log.

### P1: Scale only from complete, fresh fleet measurements

[`StatePoller.sweep`](../internal/controller/fleetstate.go) resets gauges before
sequentially polling every pod. Scrapes can observe an empty or partial fleet;
unreachable pods disappear from utilization averages. Poll time can grow by
three seconds per unreachable pod. [`promTrigger`](../internal/controller/fleet_resources.go)
does not set `ignoreNullValues`, so KEDA's default tolerates empty results.
The latency query also filters service name without destination namespace,
mixing identically named apps in different namespaces.

Publish complete snapshots atomically, bound concurrent polling, expose sample
age/completeness, and prohibit scale-down on missing or stale data. Set
`ignoreNullValues: "false"`, namespace-scope latency queries, and test Prometheus
outage, partial pod failure, and leader transitions. See the
[KEDA Prometheus scaler contract](https://keda.sh/docs/2.18/scalers/prometheus/).

### P1: Fail closed on invalid state responses at rollout gates

[`StateClient.Fetch`](../internal/controller/fleetstate.go) accepts any JSON
object that decodes into `PodState`. Missing counters become zero: `{}` or an
error object is a successful sample with `restoring=0`. celld's `actor.rs`
can return an `actor_stopped` error JSON. The rollout sweep also skips pods
that lack an IP or are not Running, and does not require a complete census
of expected old ordinals before releasing a replacement.

Validate the required safety fields and explicit runtime errors, bound response
size, and represent unavailable state distinctly from zero. Define which
unhealthy pods may safely be skipped; gate on expected membership, readiness,
and observed revision. Test malformed responses, actor failure, missing pods,
and overlapping pod termination. Introduce a persisted settle deadline if a
minimum dwell time is required: `RequeueAfter` cannot enforce one because
watch events may arrive earlier.

### P1: Define and enforce the tenant/security boundary

The fleet container has no security context and the celld v0.4 image has no
non-root USER. Pods do not meet Restricted Pod Security requirements and mount
a service-account token by default. WorkerApp writers can select arbitrary
images, reference namespace Secrets, choose IAM role annotations, and supply an
S3 endpoint. Deploy tracking can use the operator's ambient AWS credentials
against that endpoint. NetworkPolicy permits the entire operator namespace to
access the unauthenticated internal API; same-namespace fleet labels are not
an identity boundary for users allowed to create arbitrary pods.

Treat WorkerApp creation as privileged workload deployment until admission and
RBAC boundaries are implemented. Add non-root writable-volume support, seccomp,
capability drops, token opt-out with workload-identity compatibility, restricted
image/role/endpoint policies, namespace-scoped operator deployments where
needed, and narrowly selected operator ingress. Verify CNI enforcement and
encrypted peer transport in the target cluster. Do not grant broad ambient
bucket authority to an operator serving untrusted WorkerApp authors.

### P2: Separate expected deployment from observed serving version

[`updateStatus`](../internal/controller/workerapp_controller.go) sets
`rolledOutAppVersion` from the desired annotation once StatefulSet replica
counts converge. It never reads `/state.deployment.version`. celld v0.4 polls
`deploy/current.json` and adopts in place, so a pinned CR value does not pin
the running application. A stalled adoption or a second publication can make
status disagree with reality. Some API comments still describe startup-only
loading or assert that all pods serve the reported version.

Expose expected and per-pod observed deployment versions/generations, including
draining/swapping state; report convergence only after verifying live nodes.
Offer a documented in-place adoption policy versus a conservative restart
policy. Align API descriptions and generated chart schemas with that contract.
See [celld deployment behavior](https://github.com/denoland/celld/blob/v0.4.0/docs/README.md).

### P2: Make deploy tracking bounded and credential-correct

[`DeployTracker`](../internal/controller/deploytracker.go) caches by namespace
and name, without UID or bucket configuration. Deletion/recreation or a bucket
change can reuse an unrelated cached version; entries are never evicted.
Failures after TTL retry on every reconcile without a failure backoff. S3 reads
have no operator-level deadline or JSON size bound. With no static Secret the
tracker uses the operator's ambient identity, not the fleet ServiceAccount's
IRSA/AKS identity. GCS and Azure tracking are unsupported.

Key cache entries by resource identity and relevant configuration, evict deleted
apps, bound requests and response size, back off failures, and choose an explicit
read-only tracking identity per fleet. Implement store adapters or make unsupported
auto tracking an admission error. Test credential rotation and bucket migration;
consider making bucket identity immutable unless a migration workflow is selected.

### P2: Reconcile removal and actual readiness of integrations

`ensureIngress` returns early when hostnames are cleared or mode is `none`,
leaving old routes live. Changing ingress mode leaves the previous resource kind.
Merged annotations retain removed WebSocket/certificate/LB settings. Conditions
not emitted on a later pass remain in status (for example a resolved pinned
version mismatch or disabled autoscaling). Successful object creation is labeled
IngressReady without checking Gateway acceptance, references, or programming.

Track owned integration fields/resources, remove obsolete routes with ownership
checks, retire no-longer-applicable conditions, and derive readiness from actual
integration status. Avoid repeatedly updating HTTPRoute retry fields that the
standard-channel CRD prunes. Add removal/mode-switch and rejected-route tests.

### P2: Validate API invariants and runtime configuration

The API allows names too long for derived Services/labels, empty image/Secret
references, contradictory credential families, and inconsistent autoscaling
bounds. Very large `memoryGi` can overflow the int32 RSS calculation. Unknown
image tags bypass the compatibility table. CLI ingress typos silently select
HTTPRoute, and DNS assumes `cluster.local`.

Add CEL/schema checks and a documented supported image/version policy; expose
cluster domain and scheduling/startup-probe configuration. Handle existing invalid
objects through actionable conditions before tightening admission on upgrades.
Test startup under slow storage; the current TCP liveness probe has no startup
probe to protect lengthy initialization.

### P2: Make packaging and CI exercise the configuration users install

The Helm ServiceMonitor always uses HTTPS even when `metrics.insecure` selects
an HTTP Service port. Enabling cert-manager mounts certificates but does not
pass `--metrics-cert-path` to the manager, so it can continue serving a self-signed
certificate while the monitor verifies the configured CA. The chart CI builds
and loads `celld-operator:v0.1.0`, then installs the chart without overriding its
image and without waiting for readiness. It can pass without running the build.

Unify metrics TLS settings and flags; test HTTP, self-signed development, and
CA-verified configurations. Install the locally built image with `--wait` and
assert the running image. Add a real WorkerApp e2e suite covering admission,
rollout, storage recovery, NetworkPolicy, and autoscaling: the existing e2e suite
checks manager startup and metrics only. Add dependency/container vulnerability
scanning and pin build/CI tools by immutable versions. The e2e command logger also prints a temporary bearer token inside the curl
command; use mounted credentials and redact command output before reusing that
pattern outside disposable clusters. Manager memory limits
(128 MiB) and serial polling need load qualification for the intended fleet count.

## Validation

Regression tests cover API-backed no-op status/Service updates, preserved IAM
annotations, resource ownership collisions (including prior WorkerApp UIDs),
IPv4/IPv6 state URLs, and concurrent CAS writers sharing one seed ETag.
`make test` passed (Kubernetes 1.36.2 envtest), `make lint-fix` reported
zero issues, and `helm lint dist/chart` passed. The existing e2e suite passed
both tests on the dedicated `celld-operator-audit-20260910` Kind cluster with
an isolated kubeconfig; cert-manager installation was skipped. The cluster was
removed afterward. Helm certificate rendering was also inspected.
No real application durability, external identity provider, mesh, KEDA, or
production-scale load test was performed in this audit.

The final govulncheck v1.8.0 scan (Go 1.26.8) reports zero reachable affected
symbols. It still lists these advisories without detected affected call paths;
track compatible upgrades, since this result is not a proof of non-exploitability:

| Dependency | Remaining advisory | First fixed version |
| --- | --- | --- |
| cel-go v0.26.0 | [GO-2026-6094](https://pkg.go.dev/vuln/GO-2026-6094) | v0.30.0 |
| OpenTelemetry v1.43.0 | [GO-2026-5158](https://pkg.go.dev/vuln/GO-2026-5158) | v1.44.0 |
| x/mod v0.37.0 | [GO-2026-6180](https://pkg.go.dev/vuln/GO-2026-6180), [GO-2026-6179](https://pkg.go.dev/vuln/GO-2026-6179) | v0.40.0 |

`go test -race ./hack/cas-hammer` also passed. Container/base-image and Rust
runtime dependency vulnerability scans were not part of this audit.
