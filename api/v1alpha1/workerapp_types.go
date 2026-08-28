/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// The WorkerApp API follows docs/celld-behaviors.md: one WorkerApp is one celld fleet
// serving one application deployment (celld runs one app per fleet, so the
// CR, the fleet, and the app are 1:1:1).

// UpdateStrategy selects how a celld version change rolls through the fleet.
// Rolling is the partition-stepped, restoring-gated path (docs/celld-behaviors.md);
// Recreate scales to zero first, for upstream releases that forbid mixed
// fleets or flag a downgrade as lossy. A Rolling request across a
// known-breaking celld boundary is refused (F8).
// +kubebuilder:validation:Enum=Rolling;Recreate
type UpdateStrategy string

const (
	UpdateStrategyRolling  UpdateStrategy = "Rolling"
	UpdateStrategyRecreate UpdateStrategy = "Recreate"
)

// CelldSpec pins the celld runtime for the fleet.
type CelldSpec struct {
	// image is the celld container image, tag included. Mixed-version fleets
	// are never created; changing this triggers the strategy below.
	// +required
	Image string `json:"image"`

	// updateStrategy selects the rollout path for celld version changes.
	// +optional
	// +kubebuilder:default=Rolling
	UpdateStrategy UpdateStrategy `json:"updateStrategy,omitempty"`
}

// BucketCredentials selects how the fleet authenticates to its bucket
// prefix. Exactly one mechanism applies; iamRole is preferred because the
// bucket credential is fleet-admin authority and static keys spread.
type BucketCredentials struct {
	// iamRole is an IAM role ARN assumed via the pod's service account
	// (IRSA / Workload Identity), or the literal "auto" to have the
	// operator provision a role scoped to the fleet's prefix.
	// +optional
	IAMRole string `json:"iamRole,omitempty"`

	// secretRef names a Secret whose keys are injected as environment:
	// AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY for an s3:// bucket, or
	// AZURE_STORAGE_ACCOUNT_KEY for an az:// container. For stores where
	// identity-based auth is unavailable.
	// +optional
	SecretRef string `json:"secretRef,omitempty"`

	// azureClientID is the client ID of a Microsoft Entra workload identity
	// for an az:// container (AKS workload identity). The fleet
	// ServiceAccount is annotated with it and fleet pods carry the
	// azure.workload.identity/use label, so the AKS webhook injects the
	// federated token celld reads (AZURE_CLIENT_ID, AZURE_TENANT_ID,
	// AZURE_FEDERATED_TOKEN_FILE, AZURE_AUTHORITY_HOST). The identity
	// needs Storage Blob Data Contributor on the container.
	// +optional
	AzureClientID string `json:"azureClientID,omitempty"`
}

// BucketSpec locates the fleet's slice of the object store.
// +kubebuilder:validation:XValidation:rule="!self.name.startsWith('az://') || (has(self.storageAccount) && self.storageAccount.size() > 0)",message="an az:// bucket requires storageAccount (the Azure storage account; the bucket name is the container)"
// +kubebuilder:validation:XValidation:rule="!has(self.endpoint) || self.endpoint.size() == 0 || self.name.startsWith('s3://')",message="endpoint applies to s3:// buckets only; celld rejects an endpoint for gs:// and az://"
type BucketSpec struct {
	// name is the fleet bucket and prefix, e.g. "s3://platform-cells/apps/chat",
	// "gs://platform-cells/apps/chat", or "az://platform-cells/apps/chat"
	// (for az:// the bucket is a Blob Storage container in the account
	// named by storageAccount). The store must satisfy celld's fencing
	// contract (conditional create/overwrite, read-after-write); see
	// docs/celld-behaviors.md for the qualified list.
	// +required
	// +kubebuilder:validation:Pattern=`^(s3|gs|az)://.+`
	// +kubebuilder:validation:MaxLength=1024
	Name string `json:"name"`

	// endpoint is the S3-compatible endpoint URL, when not AWS S3.
	// Rejected by celld for gs:// and az:// buckets.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	Endpoint string `json:"endpoint,omitempty"`

	// storageAccount is the Azure storage account that holds an az://
	// container (AZURE_STORAGE_ACCOUNT_NAME). Required for az://, ignored
	// otherwise.
	// +optional
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=24
	// +kubebuilder:validation:Pattern=`^[a-z0-9]+$`
	StorageAccount string `json:"storageAccount,omitempty"`

	// region is the storage region, when it cannot be inferred.
	// +optional
	Region string `json:"region,omitempty"`

	// credentialsFrom selects the fleet's bucket credential.
	// +optional
	CredentialsFrom BucketCredentials `json:"credentialsFrom,omitzero"`
}

// ResourcesSpec sizes one fleet pod. The operator derives the container
// limit, CELLD_MAX_RSS_MB (~80% of the limit, set explicitly so the ceiling
// is visible in the pod spec; celld would derive the same value from the
// cgroup limit itself), and admission caps from these two numbers. celld
// applies that threshold to the memory its cells hold and keeps its own
// absolute cap at 95% of the limit on the process RSS (docs/celld-behaviors.md F10).
type ResourcesSpec struct {
	// memoryGi is the container memory limit per pod, in GiB.
	// +optional
	// +kubebuilder:default=8
	// +kubebuilder:validation:Minimum=1
	MemoryGi int32 `json:"memoryGi,omitempty"`

	// maxResidentCells is the hard per-node resident-cell admission limit
	// (CELLD_MAX_RESIDENT_CELLS). Upstream sizing: ~1000 cells per 8 GiB.
	// +optional
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=1
	MaxResidentCells int32 `json:"maxResidentCells,omitempty"`
}

// VarsSpec supplies Worker variables and secrets.
type VarsSpec struct {
	// secretRef names a Secret whose data is mounted and passed via
	// CELLD_VARS_FILE. Rotation is a Secret update plus an ordinary gated
	// rollout; values are never baked into bundles.
	// +required
	SecretRef string `json:"secretRef"`
}

// ServiceSpec shapes the serving Service that fronts the Worker listener.
// The default ClusterIP suits ingress backends and internal
// service-to-service consumers (reachable in-cluster at
// <app>-celld.<namespace>.svc:8080 with no ingress at all); LoadBalancer
// provisions a cloud LB — pair with annotations for internal/private load
// balancers; NodePort suits bare-metal edges.
type ServiceSpec struct {
	// +optional
	// +kubebuilder:validation:Enum=ClusterIP;LoadBalancer;NodePort
	// +kubebuilder:default=ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`

	// annotations merge onto the serving Service; cloud load-balancer
	// configuration (internal LB flags, protocol hints, health-check
	// tuning) lives here.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// AutoscalingTargets are the scale signals (docs/celld-behaviors.md, Autoscaling).
type AutoscalingTargets struct {
	// residentCellUtilization is the target fleet-average percentage of
	// occupied resident cells vs maxResidentCells. Kept conservative by
	// default because celld has no rebalancer and new capacity absorbs
	// slowly; any pod in pressure shedding triggers scale-up regardless.
	// +optional
	// +kubebuilder:default=70
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	ResidentCellUtilization *int32 `json:"residentCellUtilization,omitempty"`

	// p95LatencyMs adds a latency target so traffic-bound,
	// stateless-Worker-heavy apps scale even at low cell counts. The query
	// reads Istio destination telemetry, so it needs Istio metrics in the
	// same Prometheus. Unset disables the latency signal.
	// +optional
	// +kubebuilder:validation:Minimum=1
	P95LatencyMs *int32 `json:"p95LatencyMs,omitempty"`
}

// AutoscalingSpec enables custom-metrics autoscaling for the fleet. The
// operator materializes it as a KEDA ScaledObject over its own
// /state-derived Prometheus metrics, and pauses it during rollouts so the
// scaler and the partition controller never fight over replica count.
type AutoscalingSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// minReplicas is the scale floor; keep >= 2 for HA.
	// +optional
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	MinReplicas int32 `json:"minReplicas,omitempty"`

	// maxReplicas is the tenant's cost ceiling.
	// +optional
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas,omitempty"`

	// +optional
	Targets AutoscalingTargets `json:"targets,omitzero"`
}

// TelemetrySink selects where celld sends traces and logs.
// +kubebuilder:validation:Enum=bucket;otlp
type TelemetrySink string

const (
	// TelemetrySinkBucket writes Parquet under the fleet bucket's
	// telemetry/ prefix (celld's default; DuckDB-queryable, no services).
	TelemetrySinkBucket TelemetrySink = "bucket"
	// TelemetrySinkOTLP sends OTLP/HTTP protobuf to a collector.
	TelemetrySinkOTLP TelemetrySink = "otlp"
)

// TelemetrySpec controls celld's built-in tracing (CELLD_OTEL).
type TelemetrySpec struct {
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// sink selects the destination: bucket (Parquet in the fleet bucket)
	// or otlp (an OpenTelemetry collector). Unset means bucket, unless
	// otlpEndpoint is set — then otlp is inferred.
	// +optional
	Sink TelemetrySink `json:"sink,omitempty"`

	// otlpEndpoint is the collector base URL for the otlp sink
	// (OTEL_EXPORTER_OTLP_ENDPOINT), e.g. "http://otel-collector.monitoring.svc:4318".
	// Setting it without a sink selects the otlp sink.
	// +optional
	OTLPEndpoint string `json:"otlpEndpoint,omitempty"`

	// retention is CELLD_OTEL_RETENTION for the bucket sink, e.g. "30d",
	// or "none" to defer to bucket lifecycle rules. Ignored by the otlp
	// sink.
	// +optional
	// +kubebuilder:default="30d"
	Retention string `json:"retention,omitempty"`
}

// ResolvedSink is the sink after defaulting: otlp when selected or implied
// by otlpEndpoint, bucket otherwise.
func (t *TelemetrySpec) ResolvedSink() TelemetrySink {
	if t.Sink == TelemetrySinkOTLP || (t.Sink == "" && t.OTLPEndpoint != "") {
		return TelemetrySinkOTLP
	}
	return TelemetrySinkBucket
}

// Durability selects celld's write-acknowledgement proof (CELLD_DURABILITY,
// docs/celld-behaviors.md F13).
// +kubebuilder:validation:Enum=fleet;bucket
type Durability string

const (
	// DurabilityFleet (celld's default since v0.3.0) acknowledges a write
	// once two follower nodes hold it on disk, and tiers it to the bucket
	// behind. A one-node fleet behaves as bucket.
	DurabilityFleet Durability = "fleet"
	// DurabilityBucket acknowledges a write only after it is in the bucket
	// (celld's pre-0.3 behavior): higher write latency, no reliance on
	// follower disks.
	DurabilityBucket Durability = "bucket"
)

// WorkerAppSpec defines the desired state of WorkerApp.
type WorkerAppSpec struct {
	// hostnames route to this app. One route object is reconciled in the
	// app's namespace carrying every hostname; what kind depends on the
	// operator's --ingress-mode.
	// +optional
	// +listType=set
	Hostnames []string `json:"hostnames,omitempty"`

	// appVersion names the application deployment in the fleet bucket
	// (written there by `celld deploy`). Nodes load a deployment at startup
	// only, so changing this triggers the gated rollout (docs/celld-behaviors.md).
	// The sentinel "auto" makes the operator follow the bucket's
	// deploy/current.json instead: `celld deploy` alone rolls the fleet,
	// within one poll interval, with no CR edit.
	// +required
	// +kubebuilder:validation:MinLength=1
	AppVersion string `json:"appVersion"`

	// +required
	Celld CelldSpec `json:"celld"`

	// replicas is the fleet size when autoscaling is disabled, and the
	// initial size otherwise.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	Replicas *int32 `json:"replicas,omitempty"`

	// +required
	Bucket BucketSpec `json:"bucket"`

	// +optional
	Resources ResourcesSpec `json:"resources,omitzero"`

	// service shapes the serving Service (type, annotations) for internal
	// consumers and load-balancer setups.
	// +optional
	Service ServiceSpec `json:"service,omitzero"`

	// +optional
	Vars *VarsSpec `json:"vars,omitempty"`

	// websockets selects the WebSocket profile: long edge timeouts where the
	// ingress mode can express them (httproute and ingress; not
	// virtualservice) and a conservative scale-down window. No session
	// affinity is configured.
	// +optional
	WebSockets bool `json:"websockets,omitempty"`

	// +optional
	Autoscaling *AutoscalingSpec `json:"autoscaling,omitempty"`

	// +optional
	Telemetry TelemetrySpec `json:"telemetry,omitzero"`

	// durability selects how celld proves a write before acknowledging it:
	// fleet (celld's default: two follower nodes fsync it, the bucket
	// upload follows) or bucket (the write is in the bucket first). Unset
	// leaves celld's default. Changing it restarts the fleet through the
	// ordinary gated rollout.
	// +optional
	Durability Durability `json:"durability,omitempty"`

	// trustForwardedHeaders lets X-Forwarded-Host and X-Forwarded-Proto
	// set the scheme and host of request.url (CELLD_TRUST_FORWARDED_HEADERS).
	// celld ignores both headers by default, so behind a TLS-terminating
	// ingress a Worker sees its pod address as its URL. Enable it only when
	// every hop in front of the fleet replaces both headers (ingress-nginx
	// does; Envoy-based gateways set X-Forwarded-Proto but not always
	// X-Forwarded-Host), since celld takes the last value of each.
	// +optional
	TrustForwardedHeaders bool `json:"trustForwardedHeaders,omitempty"`
}

// WorkerAppPhase summarizes the fleet at a glance.
// +kubebuilder:validation:Enum=Pending;Ready;RollingOut;Recreating;Degraded
type WorkerAppPhase string

const (
	PhasePending    WorkerAppPhase = "Pending"
	PhaseReady      WorkerAppPhase = "Ready"
	PhaseRollingOut WorkerAppPhase = "RollingOut"
	PhaseRecreating WorkerAppPhase = "Recreating"
	PhaseDegraded   WorkerAppPhase = "Degraded"
)

// RolloutStatus reports the partition-stepped rollout (docs/celld-behaviors.md).
type RolloutStatus struct {
	// partition is the current StatefulSet rolling-update partition owned
	// by the rollout controller. 0 means no rollout in progress.
	// +optional
	Partition int32 `json:"partition,omitempty"`

	// waitingOn names the gate the rollout is blocked on, e.g.
	// "chat-celld-2: not ready" or "fleet: restoring=3". Empty when not
	// waiting.
	// +optional
	WaitingOn string `json:"waitingOn,omitempty"`
}

// FleetStatus aggregates the per-pod /state the operator polls. The fields
// serialize even at zero so `kubectl get` renders 0 rather than a blank.
type FleetStatus struct {
	// ready is the number of pods passing the celld health check.
	// +optional
	Ready int32 `json:"ready"`

	// restoring is the fleet-wide sum of cold routes holding or awaiting an
	// activation permit. Rollouts step only at restoring == 0.
	// +optional
	Restoring int32 `json:"restoring"`
}

// WorkerAppStatus defines the observed state of WorkerApp.
type WorkerAppStatus struct {
	// +optional
	Phase WorkerAppPhase `json:"phase,omitempty"`

	// rolledOutAppVersion is the appVersion every fleet pod is serving.
	// It trails spec.appVersion while a rollout is in flight.
	// +optional
	RolledOutAppVersion string `json:"rolledOutAppVersion,omitempty"`

	// +optional
	Rollout RolloutStatus `json:"rollout,omitzero"`

	// +optional
	Fleet FleetStatus `json:"fleet,omitzero"`

	// conditions represent the current state of the WorkerApp resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="App",type=string,JSONPath=`.status.rolledOutAppVersion`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.fleet.ready`
// +kubebuilder:printcolumn:name="Restoring",type=integer,JSONPath=`.status.fleet.restoring`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// WorkerApp is the Schema for the workerapps API
type WorkerApp struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WorkerApp
	// +required
	Spec WorkerAppSpec `json:"spec"`

	// status defines the observed state of WorkerApp
	// +optional
	Status WorkerAppStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkerAppList contains a list of WorkerApp
type WorkerAppList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkerApp `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &WorkerApp{}, &WorkerAppList{})
		return nil
	})
}
