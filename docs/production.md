# Production operation and migration

## Storage and rollout policy

New WorkerApps default to `durability: bucket`: acknowledgement depends on the
bucket rather than the availability of replicated local logs. Local state may
use emptyDir in this mode. To opt into fleet acknowledgement:

```yaml
spec:
  durability: fleet
  replicas: 3
  storage:
    sizeGi: 100
    storageClassName: your-durable-class
  scheduling:
    spreadAcrossZones: true
  resources:
    memoryGi: 8
    cpuMillis: 2000
    ephemeralStorageGi: 10
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10
```

Persistent fleets require distinct hosts. With `spreadAcrossZones`, each pod
also requires a distinct zone: provision enough zones for the maximum replica
count, or leave zone separation disabled and accept that failure domain.
Use a storage class whose failure/recovery guarantees satisfy your workload;
a PVC backed by node-local storage does not survive permanent node loss.
PVCs are retained after scale-down and WorkerApp deletion. Inventory and delete
them only after an explicit data-retention decision. Reusing an application
name can reuse its retained claims; do not do so for an unrelated application.

Bucket identity and storage configuration are immutable. Existing ephemeral
StatefulSets cannot gain volume claim templates through an in-place update.
Before upgrading an existing fleet-mode app, plan a maintenance migration:
stop writes, verify the runtime has uploaded/sealed acknowledged data, stop the
old fleet, provision a new WorkerApp with the intended storage, and only then
resume traffic. Never overlap fleets sharing a bucket prefix. Do not delete
local state as a substitute for verifying bucket recovery.

KEDA v2.12.0 or newer is required for the Paused=True acknowledgement.
The operator pauses KEDA and waits for its acknowledgement and HPA removal before
rolling changes. A stalled pause blocks rollout. Unmanaged HPAs targeting the
same StatefulSet also block it. Recreate waits for all old pods, including
terminating pods, to disappear. It cannot prove a killed node sealed its log;
downgrades from v0.3+ to older runtimes are therefore blocked even with Recreate.

A TCP startup probe allows ten minutes before liveness takes over. Termination
grace is 60 seconds around celld's 40-second total shutdown bound. Slow storage,
startup and failure recovery need testing with your actual runtime and store.

## Application deployments

`appVersion` is the expected bucket deployment, not a pin on what celld serves.
celld v0.4 independently follows the bucket pointer. `deploymentPolicy: Restart`
(the default) rolls pods when the expectation changes; `InPlace` lets celld
adopt without that restart. Other pod configuration changes still roll.
`status.expectedAppVersion` records the expectation; `status.deployments` records
observed per-pod versions/generations and transition counts.
`rolledOutAppVersion` is set only when every expected pod serves the expectation
and no generations are draining or swapping. Older runtimes without deployment
state cannot satisfy that gate; use a qualified v0.4 runtime for this contract.

For auto mode, supply a dedicated read-only S3 tracking credential:

```yaml
spec:
  appVersion: auto
  deployTrackingSecretRef: chat-deploy-reader
```

The Secret uses `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally
`AWS_SESSION_TOKEN`. Grant read access to the fleet's `deploy/current.json`.
Tracking never uses the manager's ambient identity or assumes the fleet's IRSA
role. Pinned mode can use the same Secret for drift warnings. GCS/Azure fleets
must use an explicit expected version; auto mode is rejected.

## Security and installation

Runtime image tags must contain a numeric major.minor.patch version, including
when digest-pinned. Default image policy permits `ghcr.io/denoland/celld:`;
configure trusted prefixes for mirrors or other builds. Unknown live versions
require an explicit Recreate transition. Explicit cloud identities and custom
HTTPS bucket endpoints require exact allowlists:

```yaml
operator:
  watchNamespace: tenant-acme
  allowedImagePrefixes: ["ghcr.io/denoland/celld:"]
  allowedIAMRoles: ["arn:aws:iam::123456789012:role/celld-chat"]
  allowedAzureClientIDs: []
  allowedBucketEndpoints: ["https://ACCOUNT.r2.cloudflarestorage.com"]
```

`watchNamespace` scopes the cache and the chart's workload Role. For a shared
Gateway in another namespace, additionally grant the manager ServiceAccount
`get` on that Gateway through a Role/RoleBinding there; otherwise readiness is
Unknown. The manager still needs its metrics authentication and leader-election
permissions. An empty namespace retains cluster-wide workload reconciliation.

WorkerApp authors are trusted workload deployers. Restrict their Kubernetes
permissions and isolate unrelated tenants in namespaces with separate bucket
credentials. Image allowlists do not sandbox arbitrary Worker code. Pods use
UID/GID 10001, read-only root filesystems and writable `/tmp`/watch volumes;
qualify your runtime image, CSI permissions and identity injector accordingly.
No default service-account token is mounted, but identity webhooks may inject
purpose-specific tokens. NetworkPolicy requires selected manager labels; retain
`control-plane: controller-manager` and `app.kubernetes.io/name: celld-operator`
on manager pods. Verify CNI enforcement and peer-network encryption separately.

`spec.clusterDomain` supports non-default cluster DNS suffixes. Set the Istio
operator principal separately when its trust domain differs. Runtime listeners
still bind IPv4; IPv6-only pod networking is not currently supported.

## Ingress, metrics and capacity

HTTPRoute readiness requires fresh Accepted and ResolvedRefs conditions and a
Programmed Gateway. Ingress/VirtualService creation reports Unknown readiness,
because object creation alone does not prove traffic can flow. Removing hosts
or changing ingress modes removes obsolete owned routes.

HTTPRoute retries default off for standard Gateway API CRDs. Enable
`operator.httpRouteRetries` only with experimental CRDs and a supporting gateway.
If the API prunes retries, the controller reports failure and stops rewriting
them. Support is retried automatically after a persisted five-minute delay, so upgrading
the CRDs does not require manually clearing the retry latch. Clients
must tolerate rollout 503s unless their ingress is configured to retry them;
WebSocket connections may still close during node replacement.

The chart derives manager and ServiceMonitor TLS settings from `metrics` and
`certmanager`. Insecure mode uses HTTP; cert-manager mode supplies the certificate
path and CA verification. Do not combine cert-manager with insecure metrics.
The monitor preserves exported namespace/pod labels with `honorLabels`.

Scaling queries require `celld_fleet_complete = 1` and a
`celld_fleet_sample_timestamp_seconds` younger than 60 seconds. Partial data or
an unavailable Prometheus produces a scaler error instead of a low utilization
sample. Alert on these errors and stale/absent metrics. Keep polling intervals
comfortably below 60 seconds. Polls have 16-way concurrency and a 30-second sweep
budget; large or unhealthy fleets may fail completeness rather than extending
that deadline. Size manager CPU/memory and replica limits using a load test;
the chart's 512 MiB limit is a starting configuration, not a capacity guarantee.
