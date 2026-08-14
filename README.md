# celld-operator

A Kubernetes operator that turns [celld](https://github.com/denoland/celld) —
Deno Land's self-hosted Cloudflare Workers & Durable Objects runtime — into a
platform: one `WorkerApp` custom resource per application provisions and
operates a complete celld fleet.

Read [DESIGN.md](DESIGN.md) first; every behavior below is grounded there.

## What one WorkerApp reconciles to

- **StatefulSet** running the celld fleet, with the guardrails celld's docs
  require baked in: stable per-pod advertise DNS via a headless Service,
  readiness on `/__celld/health` (which goes 503 during a graceful drain),
  TCP-only liveness (an HTTP liveness probe on the health path would kill
  draining nodes), termination grace above `CELLD_SHUTDOWN_DRAIN_MS`, and an
  explicit `CELLD_MAX_RSS_MB` derived from the container memory limit.
- **Services**: public (`:8080`, the Worker) and headless internal (`:8081`,
  peer protocol + operator API, `publishNotReadyAddresses` so peers can hand
  off cells during drains).
- **NetworkPolicy** restricting `:8081` — celld's operator API is
  unauthenticated upstream — to fleet pods and the operator's namespace, plus
  an Istio **AuthorizationPolicy** with the same intent when Istio is present.
- **PodDisruptionBudget** (`maxUnavailable: 1`) so node maintenance drains
  serially.
- **HTTPRoute** on a shared Gateway (`--gateway-name` / `--gateway-namespace`)
  for `spec.hostnames`, with 503 retries so drains are invisible to clients
  and, for `websockets: true` apps, a disabled request timeout.
- **KEDA ScaledObject** (when `spec.autoscaling.enabled`) over the operator's
  own metrics, paused automatically during rollouts.

Gateway API, Istio, and KEDA are all optional: a missing CRD is reported as a
status condition, never a reconcile failure.

## Rollouts

celld nodes load their application deployment from the fleet bucket **at
startup only**, and celld's docs require waiting for fleet-wide `restoring=0`
between node restarts — work that lands on the *peers* of the restarted node,
which a vanilla rolling update cannot see. So the operator owns the
StatefulSet partition and steps it one ordinal at a time, gating each step on
pod readiness **and** a live sweep of every pod's `/state`.

Deploying an app is therefore: `celld deploy` to the bucket, then bump
`spec.appVersion` — the operator rolls the fleet.

celld version bumps roll the same way, except across boundaries upstream
flags as not rolling-safe (e.g. v0.1 → v0.2): those are refused unless
`spec.celld.updateStrategy: Recreate` is set, which scales to zero and back —
an availability event the CR must ask for explicitly.

## Metrics

celld exports no metrics yet, so the operator polls each fleet pod's internal
`/state` and re-exports it as Prometheus metrics: `celld_resident_cells`,
`celld_resident_cell_utilization` (the primary autoscaling signal),
`celld_restoring`, `celld_evicting`, `celld_shedding`, `celld_state_up`.

## Store qualification

celld's fencing needs conditional writes the store must enforce atomically;
a store that accepts the headers without enforcing them splits ownership
silently. `hack/cas-hammer` races N concurrent conditional PUTs per round and
asserts exactly one winner:

```sh
go run ./hack/cas-hammer --bucket my-bucket --endpoint https://... --writers 8 --rounds 32
```

Run it (plus celld's own `put_cas_contract` test) against any store not on
the qualified list in DESIGN.md §9, and again on every store upgrade.

## Development

```sh
make test     # envtest suite
make lint     # golangci-lint
make run      # run the operator against the current kubeconfig
```

Sample CR: [config/samples/platform_v1alpha1_workerapp.yaml](config/samples/platform_v1alpha1_workerapp.yaml).

## License

Apache-2.0
