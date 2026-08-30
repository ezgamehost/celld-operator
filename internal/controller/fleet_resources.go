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

package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
)

// Builders for everything one WorkerApp fleet reconciles to (docs/celld-behaviors.md).
// Naming: every child object is "<app>-celld"; the headless peer service is
// "<app>-celld-internal". Pods carry the workerapp label, which is the
// selector for the StatefulSet, both Services, the PDB, the NetworkPolicy,
// and the Istio AuthorizationPolicy.

const (
	celldContainerName = "celld"

	// iamRoleAuto asks the operator to provision the role (not implemented;
	// surfaced as a condition).
	iamRoleAuto = "auto"
	// defaultBucketRegion suits R2 and other region-less S3 endpoints.
	defaultBucketRegion = "auto"

	workerAppLabel = "celld-operator.io/workerapp"

	appVersionAnnotation   = "celld-operator.io/app-version"
	templateHashAnnotation = "celld-operator.io/template-hash"
	kedaPausedAnnotation   = "autoscaling.keda.sh/paused"

	publicPort   = 8080
	internalPort = 8081

	// celld's default drain bound; terminationGracePeriodSeconds must exceed
	// it so the orchestrator never SIGKILLs a draining node (docs/celld-behaviors.md F6).
	shutdownDrainMs   = 25000
	terminationGraceS = 40

	watchDir = "/var/lib/celld"
	varsDir  = "/etc/celld/vars"
	// The vars Secret must carry its variables under this key. The file is
	// handed to celld via CELLD_VARS_FILE and is env-file format —
	// NAME=value lines, # comments, optional quotes (see celld's
	// fleet.rs worker_vars) — not JSON.
	varsKey = "vars.env"

	// configHashAnnotation carries a digest of every Secret the pod
	// template references (vars, bucket credentials). Rotating a Secret
	// changes the digest, which changes the template, which triggers the
	// ordinary gated rollout — without it, rotation silently did nothing
	// until some unrelated rollout happened to restart the fleet.
	configHashAnnotation = "celld-operator.io/config-hash"

	// AKS workload identity: the admission webhook injects the federated
	// token environment into pods carrying the use label whose
	// ServiceAccount names a client ID. celld reads exactly that
	// environment for an az:// container (docs/celld-behaviors.md F7).
	azureWorkloadIdentityUseLabel = "azure.workload.identity/use"
	azureClientIDAnnotation       = "azure.workload.identity/client-id"
)

func isAzureBucket(app *platformv1alpha1.WorkerApp) bool {
	return strings.HasPrefix(app.Spec.Bucket.Name, "az://")
}

func fleetName(app *platformv1alpha1.WorkerApp) string {
	return app.Name + "-celld"
}

func internalServiceName(app *platformv1alpha1.WorkerApp) string {
	return app.Name + "-celld-internal"
}

func fleetLabels(app *platformv1alpha1.WorkerApp) map[string]string {
	return map[string]string{
		workerAppLabel:               app.Name,
		"app.kubernetes.io/name":     celldContainerName,
		"app.kubernetes.io/instance": app.Name,
	}
}

func selectorLabels(app *platformv1alpha1.WorkerApp) map[string]string {
	return map[string]string{workerAppLabel: app.Name}
}

func desiredReplicas(app *platformv1alpha1.WorkerApp) int32 {
	if app.Spec.Replicas != nil {
		return *app.Spec.Replicas
	}
	return 3
}

func memoryGi(app *platformv1alpha1.WorkerApp) int32 {
	if app.Spec.Resources.MemoryGi > 0 {
		return app.Spec.Resources.MemoryGi
	}
	return 8
}

func maxResidentCells(app *platformv1alpha1.WorkerApp) int32 {
	if app.Spec.Resources.MaxResidentCells > 0 {
		return app.Spec.Resources.MaxResidentCells
	}
	return 1000
}

// buildPodTemplate is the single source of truth for a fleet pod. The
// rollout controller compares its hash against the live StatefulSet to
// decide whether a gated rollout is needed (docs/celld-behaviors.md). configHash
// digests the referenced Secrets so rotation rolls the fleet; appVersion
// is the resolved deployment version (pinned or bucket-tracked).
func buildPodTemplate(app *platformv1alpha1.WorkerApp, configHash, appVersion string) corev1.PodTemplateSpec {
	memGi := memoryGi(app)
	// Explicit shed threshold ≈80% of the container limit. celld computes
	// the same 80% from the cgroup limit on its own; setting it makes the
	// ceiling visible in the pod spec and stable if that derivation ever
	// changes. It stays under celld's own absolute RSS cap (95% of the
	// limit), so the cell-memory threshold keeps the recovery property of
	// shedding (docs/celld-behaviors.md F10).
	rssMb := memGi * 1024 * 4 / 5

	env := []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
		{Name: "CELLD_BUCKET", Value: app.Spec.Bucket.Name},
		{Name: "CELLD_INTERNAL_ADDR", Value: fmt.Sprintf("0.0.0.0:%d", internalPort)},
		// Stable per-pod DNS via the headless service; peers resolve it to
		// the internal listener (F2).
		{Name: "CELLD_ADVERTISE", Value: fmt.Sprintf(
			"$(POD_NAME).%s.%s.svc.cluster.local:%d",
			internalServiceName(app), app.Namespace, internalPort)},
		{Name: "CELLD_WATCH", Value: watchDir},
		{Name: "CELLD_SHUTDOWN_DRAIN_MS", Value: fmt.Sprintf("%d", shutdownDrainMs)},
		{Name: "CELLD_MAX_RESIDENT_CELLS", Value: fmt.Sprintf("%d", maxResidentCells(app))},
		{Name: "CELLD_MAX_RSS_MB", Value: fmt.Sprintf("%d", rssMb)},
	}
	if app.Spec.Bucket.Endpoint != "" {
		env = append(env, corev1.EnvVar{Name: "S3_ENDPOINT", Value: app.Spec.Bucket.Endpoint})
	}
	if app.Spec.Bucket.Region != "" {
		env = append(env, corev1.EnvVar{Name: "AWS_REGION", Value: app.Spec.Bucket.Region})
	}
	if isAzureBucket(app) && app.Spec.Bucket.StorageAccount != "" {
		// celld takes the account from the environment; the bucket name
		// is the container (F7). A credential Secret may carry the same
		// key — `env` wins over `envFrom`, and the CRD requires this
		// field for az://, so the pod spec is the one source of truth.
		env = append(env, corev1.EnvVar{Name: "AZURE_STORAGE_ACCOUNT_NAME", Value: app.Spec.Bucket.StorageAccount})
	}
	if app.Spec.Durability != "" {
		// Unset keeps celld's default (fleet since v0.3.0), so the fleet
		// follows upstream unless the CR pins a proof (F13).
		env = append(env, corev1.EnvVar{Name: "CELLD_DURABILITY", Value: string(app.Spec.Durability)})
	}
	if app.Spec.TrustForwardedHeaders {
		// Off unless asked: celld reads the last value of each header, so
		// this is only safe when every proxy hop replaces both.
		env = append(env, corev1.EnvVar{Name: "CELLD_TRUST_FORWARDED_HEADERS", Value: "1"})
	}
	telemetryOn := app.Spec.Telemetry.Enabled == nil || *app.Spec.Telemetry.Enabled
	if telemetryOn {
		env = append(env, corev1.EnvVar{Name: "CELLD_OTEL", Value: "1"})
		// Per-app service name: on a shared collector, "celld" (the
		// upstream default) would flatten every fleet into one service.
		env = append(env, corev1.EnvVar{Name: "OTEL_SERVICE_NAME", Value: app.Name})
		if app.Spec.Telemetry.ResolvedSink() == platformv1alpha1.TelemetrySinkOTLP {
			env = append(env, corev1.EnvVar{Name: "CELLD_OTEL_SINK", Value: "otlp"})
			if app.Spec.Telemetry.OTLPEndpoint != "" {
				env = append(env, corev1.EnvVar{
					Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: app.Spec.Telemetry.OTLPEndpoint,
				})
			}
		} else if app.Spec.Telemetry.Retention != "" {
			env = append(env, corev1.EnvVar{Name: "CELLD_OTEL_RETENTION", Value: app.Spec.Telemetry.Retention})
		}
	} else {
		env = append(env, corev1.EnvVar{Name: "CELLD_OTEL", Value: "0"})
	}

	volumes := []corev1.Volume{{
		Name:         "watch",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
	mounts := []corev1.VolumeMount{{Name: "watch", MountPath: watchDir}}

	if app.Spec.Vars != nil {
		volumes = append(volumes, corev1.Volume{
			Name: "vars",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: app.Spec.Vars.SecretRef,
			}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "vars", MountPath: varsDir, ReadOnly: true})
		env = append(env, corev1.EnvVar{Name: "CELLD_VARS_FILE", Value: varsDir + "/" + varsKey})
	}

	var envFrom []corev1.EnvFromSource
	if ref := app.Spec.Bucket.CredentialsFrom.SecretRef; ref != "" {
		envFrom = append(envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: ref},
			},
		})
	}

	container := corev1.Container{
		Name:  celldContainerName,
		Image: app.Spec.Celld.Image,
		Args: []string{
			"--bucket", app.Spec.Bucket.Name,
			"--listen", fmt.Sprintf("0.0.0.0:%d", publicPort),
		},
		Env:     env,
		EnvFrom: envFrom,
		Ports: []corev1.ContainerPort{
			{Name: "public", ContainerPort: publicPort},
			{Name: "internal", ContainerPort: internalPort},
		},
		// Readiness on celld's health path: it answers 503 during a drain,
		// which pulls the pod from EndpointSlices — the built-in drain
		// signal (F6).
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path: "/__celld/health",
				Port: intstr.FromInt32(publicPort),
			}},
			PeriodSeconds:    5,
			FailureThreshold: 3,
		},
		// Liveness must NOT use the health path: it goes 503 during a
		// graceful drain, and an HTTP liveness probe there kills the node
		// mid-handoff (docs/celld-behaviors.md). TCP only.
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt32(publicPort),
			}},
			PeriodSeconds:    20,
			FailureThreshold: 3,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", memGi)),
			},
		},
		VolumeMounts: mounts,
	}

	annotations := map[string]string{}
	if appVersion != "" {
		// The declarative rollout trigger (F4): `celld deploy` publishes
		// to the bucket, then this annotation changing (a spec bump, or
		// the tracked pointer moving) rolls the fleet so nodes restart
		// into the new deployment.
		annotations[appVersionAnnotation] = appVersion
	}
	if configHash != "" {
		annotations[configHashAnnotation] = configHash
	}

	labels := fleetLabels(app)
	if app.Spec.Bucket.CredentialsFrom.AzureClientID != "" {
		// Pod-only label (never part of the selector): it is what makes
		// the AKS webhook mutate the pod.
		labels[azureWorkloadIdentityUseLabel] = "true"
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:            fleetName(app),
			TerminationGracePeriodSeconds: ptr.To(int64(terminationGraceS)),
			Containers:                    []corev1.Container{container},
			Volumes:                       volumes,
			// Fleet durability (F13) acknowledges a write once two follower
			// nodes hold it on their local disk; the bucket upload trails.
			// Until it lands, that write exists on three pods' emptyDirs,
			// so co-locating them turns one host failure into lost
			// acknowledged writes. Spread across hosts — soft, so a
			// single-node dev cluster still schedules.
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew:           1,
				TopologyKey:       "kubernetes.io/hostname",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: selectorLabels(app)},
			}},
		},
	}
}

// templateHash fingerprints the desired pod template. Comparing hashes
// (rather than deep-equality against the server-defaulted live object)
// decides when a rollout starts.
func templateHash(t corev1.PodTemplateSpec) string {
	raw, err := json.Marshal(t)
	if err != nil {
		// Marshaling our own built struct cannot fail; keep the signature
		// simple and make any future surprise loud in the hash comparison.
		return "marshal-error"
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

func buildStatefulSet(app *platformv1alpha1.WorkerApp, configHash, appVersion string) *appsv1.StatefulSet {
	template := buildPodTemplate(app, configHash, appVersion)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fleetName(app),
			Namespace: app.Namespace,
			Labels:    fleetLabels(app),
			Annotations: map[string]string{
				templateHashAnnotation: templateHash(template),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: internalServiceName(app),
			Replicas:    ptr.To(desiredReplicas(app)),
			Selector:    &metav1.LabelSelector{MatchLabels: selectorLabels(app)},
			Template:    template,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					Partition: ptr.To(int32(0)),
				},
			},
		},
	}
}

func buildInternalService(app *platformv1alpha1.WorkerApp) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      internalServiceName(app),
			Namespace: app.Namespace,
			Labels:    fleetLabels(app),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  selectorLabels(app),
			// Peers must reach a draining or not-yet-ready node to hand off
			// and take over cells; readiness gating would break that.
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{{
				Name: "internal", Port: internalPort, TargetPort: intstr.FromInt32(internalPort),
			}},
		},
	}
}

func buildPublicService(app *platformv1alpha1.WorkerApp) *corev1.Service {
	serviceType := app.Spec.Service.Type
	if serviceType == "" {
		serviceType = corev1.ServiceTypeClusterIP
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fleetName(app),
			Namespace:   app.Namespace,
			Labels:      fleetLabels(app),
			Annotations: app.Spec.Service.Annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: selectorLabels(app),
			Ports: []corev1.ServicePort{{
				Name: "public", Port: publicPort, TargetPort: intstr.FromInt32(publicPort),
			}},
		},
	}
}

func buildServiceAccount(app *platformv1alpha1.WorkerApp) *corev1.ServiceAccount {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fleetName(app),
			Namespace: app.Namespace,
			Labels:    fleetLabels(app),
		},
	}
	// The bucket credential is fleet-admin authority, so it arrives through
	// the pod identity, scoped to this fleet's prefix (docs/celld-behaviors.md F7):
	// IRSA on EKS, workload identity on AKS. GKE Workload Identity wiring
	// is a v1 item; "auto" provisioning is docs/celld-behaviors.md "known
	// not-implemented" and is surfaced as a condition.
	annotations := map[string]string{}
	if role := app.Spec.Bucket.CredentialsFrom.IAMRole; role != "" && role != iamRoleAuto {
		annotations["eks.amazonaws.com/role-arn"] = role
	}
	if clientID := app.Spec.Bucket.CredentialsFrom.AzureClientID; clientID != "" {
		annotations[azureClientIDAnnotation] = clientID
	}
	if len(annotations) > 0 {
		sa.Annotations = annotations
	}
	return sa
}

// buildNetworkPolicy locks the internal listener down: celld's operator API
// on :8081 is unauthenticated upstream (F1), so only fleet pods and the
// operator's namespace may reach it. :8080 stays open — the gateway (and
// mesh policy, where present) fronts it.
func buildNetworkPolicy(app *platformv1alpha1.WorkerApp, operatorNamespace string) *networkingv1.NetworkPolicy {
	protoTCP := corev1.ProtocolTCP
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fleetName(app),
			Namespace: app.Namespace,
			Labels:    fleetLabels(app),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: selectorLabels(app)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					Ports: []networkingv1.NetworkPolicyPort{{
						Protocol: &protoTCP,
						Port:     ptr.To(intstr.FromInt32(internalPort)),
					}},
					From: []networkingv1.NetworkPolicyPeer{
						{PodSelector: &metav1.LabelSelector{MatchLabels: selectorLabels(app)}},
						{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
							"kubernetes.io/metadata.name": operatorNamespace,
						}}},
					},
				},
				{
					Ports: []networkingv1.NetworkPolicyPort{{
						Protocol: &protoTCP,
						Port:     ptr.To(intstr.FromInt32(publicPort)),
					}},
				},
			},
		},
	}
}

// buildPDB keeps voluntary disruptions serialized so node maintenance drains
// one fleet pod at a time, matching the rollout discipline.
func buildPDB(app *platformv1alpha1.WorkerApp) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fleetName(app),
			Namespace: app.Namespace,
			Labels:    fleetLabels(app),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: ptr.To(intstr.FromInt32(1)),
			Selector:       &metav1.LabelSelector{MatchLabels: selectorLabels(app)},
		},
	}
}

// buildHTTPRoute binds the app's hostnames to the public Service on the
// shared Gateway. Route policy per docs/celld-behaviors.md: retry the drain 503s so
// rollouts are invisible to clients, and disable the request timeout for
// WebSocket apps so quiet hibernated sockets are not severed.
func buildHTTPRoute(app *platformv1alpha1.WorkerApp, gatewayName, gatewayNamespace string) *gatewayv1.HTTPRoute {
	hostnames := make([]gatewayv1.Hostname, 0, len(app.Spec.Hostnames))
	for _, h := range app.Spec.Hostnames {
		hostnames = append(hostnames, gatewayv1.Hostname(h))
	}
	rule := gatewayv1.HTTPRouteRule{
		BackendRefs: []gatewayv1.HTTPBackendRef{{
			BackendRef: gatewayv1.BackendRef{
				BackendObjectReference: gatewayv1.BackendObjectReference{
					Name: gatewayv1.ObjectName(fleetName(app)),
					Port: ptr.To(gatewayv1.PortNumber(publicPort)),
				},
			},
		}},
		Retry: &gatewayv1.HTTPRouteRetry{
			Codes:    []gatewayv1.HTTPRouteRetryStatusCode{503},
			Attempts: ptr.To(2),
		},
	}
	if app.Spec.WebSockets {
		rule.Timeouts = &gatewayv1.HTTPRouteTimeouts{
			Request: ptr.To(gatewayv1.Duration("0s")),
		}
	}
	return &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fleetName(app),
			Namespace: app.Namespace,
			Labels:    fleetLabels(app),
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{{
					Name:      gatewayv1.ObjectName(gatewayName),
					Namespace: ptr.To(gatewayv1.Namespace(gatewayNamespace)),
				}},
			},
			Hostnames: hostnames,
			Rules:     []gatewayv1.HTTPRouteRule{rule},
		},
	}
}

// newUnstructuredObject assembles the envelope for CRDs the operator does
// not link types for (Istio, KEDA), so their absence never breaks the build
// or the reconcile.
func newUnstructuredObject(apiVersion, kind, name, namespace string, labels, annotations map[string]any, spec map[string]any) *unstructured.Unstructured {
	metadata := map[string]any{
		"name":      name,
		"namespace": namespace,
		"labels":    labels,
	}
	if annotations != nil {
		metadata["annotations"] = annotations
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   metadata,
		"spec":       spec,
	}}
}

// buildIngress is the networking.k8s.io/v1 counterpart of buildHTTPRoute,
// for clusters fronted by a classic ingress controller
// (--ingress-mode=ingress). The route policy carries over as annotations:
// drain 503s are retried and WebSocket routes get long proxy timeouts —
// expressed in ingress-nginx vocabulary, ignored by other controllers.
// With --cluster-issuer set, cert-manager issues per-app TLS via the
// standard annotation.
func buildIngress(app *platformv1alpha1.WorkerApp, className, clusterIssuer string) *networkingv1.Ingress {
	pathType := networkingv1.PathTypePrefix
	rules := make([]networkingv1.IngressRule, 0, len(app.Spec.Hostnames))
	for _, host := range app.Spec.Hostnames {
		rules = append(rules, networkingv1.IngressRule{
			Host: host,
			IngressRuleValue: networkingv1.IngressRuleValue{
				HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path:     "/",
						PathType: &pathType,
						Backend: networkingv1.IngressBackend{
							Service: &networkingv1.IngressServiceBackend{
								Name: fleetName(app),
								Port: networkingv1.ServiceBackendPort{Number: publicPort},
							},
						},
					}},
				},
			},
		})
	}

	annotations := map[string]string{
		// A draining celld node answers new requests with 503+close and
		// expects the client to retry on a healthy node (F6); make the
		// ingress controller be that client.
		"nginx.ingress.kubernetes.io/proxy-next-upstream":       "error timeout http_503",
		"nginx.ingress.kubernetes.io/proxy-next-upstream-tries": "3",
	}
	if app.Spec.WebSockets {
		// Long-lived, possibly hibernated sockets must not be severed by
		// proxy idle timeouts; clients keep them warm with pings answered
		// by setWebSocketAutoResponse without waking the cell.
		annotations["nginx.ingress.kubernetes.io/proxy-read-timeout"] = "3600"
		annotations["nginx.ingress.kubernetes.io/proxy-send-timeout"] = "3600"
	}

	spec := networkingv1.IngressSpec{Rules: rules}
	if className != "" {
		spec.IngressClassName = &className
	}
	if clusterIssuer != "" {
		annotations["cert-manager.io/cluster-issuer"] = clusterIssuer
		hosts := make([]string, len(app.Spec.Hostnames))
		copy(hosts, app.Spec.Hostnames)
		spec.TLS = []networkingv1.IngressTLS{{
			Hosts:      hosts,
			SecretName: fleetName(app) + "-tls",
		}}
	}

	return &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fleetName(app),
			Namespace:   app.Namespace,
			Labels:      fleetLabels(app),
			Annotations: annotations,
		},
		Spec: spec,
	}
}

// buildVirtualService is the classic-Istio counterpart of buildHTTPRoute,
// for clusters whose ingress is an existing istio-ingressgateway rather
// than a Gateway API implementation (--ingress-mode=virtualservice). Same
// route policy: retry the drain 503s so rollouts stay invisible to
// clients. It binds to pre-existing gateways named by the operator flags;
// gateway lifecycle stays out of the operator, matching the shared-Gateway
// model.
func buildVirtualService(app *platformv1alpha1.WorkerApp, gateways []string) *unstructured.Unstructured {
	hosts := make([]any, 0, len(app.Spec.Hostnames))
	for _, h := range app.Spec.Hostnames {
		hosts = append(hosts, h)
	}
	gws := make([]any, 0, len(gateways))
	for _, g := range gateways {
		gws = append(gws, g)
	}
	return newUnstructuredObject(
		"networking.istio.io/v1", "VirtualService",
		fleetName(app), app.Namespace,
		toAnyMap(fleetLabels(app)), nil,
		map[string]any{
			"hosts":    hosts,
			"gateways": gws,
			"http": []any{map[string]any{
				"route": []any{map[string]any{"destination": map[string]any{
					"host": fmt.Sprintf("%s.%s.svc.cluster.local", fleetName(app), app.Namespace),
					"port": map[string]any{"number": int64(publicPort)},
				}}},
				"retries": map[string]any{
					"attempts": int64(2),
					"retryOn":  "connect-failure,refused-stream,503",
				},
			}},
		},
	)
}

// buildAuthorizationPolicy adds identity-based enforcement in front of the
// unauthenticated operator API when Istio is present: only the fleet's own
// service account and the operator may speak to :8081 (docs/celld-behaviors.md).
// Unstructured so the operator does not depend on Istio being installed.
func buildAuthorizationPolicy(app *platformv1alpha1.WorkerApp, operatorPrincipal string) *unstructured.Unstructured {
	fleetPrincipal := fmt.Sprintf("cluster.local/ns/%s/sa/%s", app.Namespace, fleetName(app))
	return newUnstructuredObject(
		"security.istio.io/v1", "AuthorizationPolicy",
		fleetName(app)+"-internal", app.Namespace,
		toAnyMap(fleetLabels(app)), nil,
		map[string]any{
			"selector": map[string]any{"matchLabels": toAnyMap(selectorLabels(app))},
			"action":   "ALLOW",
			"rules": []any{
				map[string]any{
					"from": []any{map[string]any{"source": map[string]any{
						"principals": []any{fleetPrincipal, operatorPrincipal},
					}}},
					"to": []any{map[string]any{"operation": map[string]any{
						"ports": []any{fmt.Sprintf("%d", internalPort)},
					}}},
				},
				map[string]any{
					"to": []any{map[string]any{"operation": map[string]any{
						"ports": []any{fmt.Sprintf("%d", publicPort)},
					}}},
				},
			},
		},
	)
}

// buildScaledObject materializes spec.autoscaling as a KEDA ScaledObject
// over the operator's /state-derived metrics (docs/celld-behaviors.md, Autoscaling).
// paused pins replicas during rollouts so the scaler and the partition
// controller never fight.
func buildScaledObject(app *platformv1alpha1.WorkerApp, prometheusURL string, paused bool) *unstructured.Unstructured {
	as := app.Spec.Autoscaling
	minReplicas := as.MinReplicas
	if minReplicas == 0 {
		minReplicas = 2
	}
	maxReplicas := as.MaxReplicas
	if maxReplicas == 0 {
		maxReplicas = 10
	}
	target := int32(70)
	if as.Targets.ResidentCellUtilization != nil {
		target = *as.Targets.ResidentCellUtilization
	}

	triggers := []any{
		promTrigger(prometheusURL,
			fmt.Sprintf(`avg(celld_resident_cell_utilization{namespace=%q,workerapp=%q})`,
				app.Namespace, app.Name),
			fmt.Sprintf("%.2f", float64(target)/100)),
		// The shedding fast path: any pod refusing new cells means the
		// fleet is already rebalancing the hard way — scale immediately.
		promTrigger(prometheusURL,
			fmt.Sprintf(`max(celld_shedding{namespace=%q,workerapp=%q})`,
				app.Namespace, app.Name),
			"0.5"),
	}
	if as.Targets.P95LatencyMs != nil {
		triggers = append(triggers, promTrigger(prometheusURL,
			fmt.Sprintf(`histogram_quantile(0.95, sum(rate(istio_request_duration_milliseconds_bucket{destination_service_name=%q,reporter="destination"}[5m])) by (le))`,
				fleetName(app)),
			fmt.Sprintf("%d", *as.Targets.P95LatencyMs)))
	}

	// Scale down slowly and one pod at a time: removing a pod hands off its
	// cells (cold restores on peers) and closes its WebSockets; WebSocket
	// fleets wait even longer (docs/celld-behaviors.md).
	stabilizationS := 600
	if app.Spec.WebSockets {
		stabilizationS = 1800
	}

	return newUnstructuredObject(
		"keda.sh/v1alpha1", "ScaledObject",
		fleetName(app), app.Namespace,
		toAnyMap(fleetLabels(app)),
		map[string]any{kedaPausedAnnotation: fmt.Sprintf("%t", paused)},
		map[string]any{
			"scaleTargetRef": map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "StatefulSet",
				"name":       fleetName(app),
			},
			"minReplicaCount": int64(minReplicas),
			"maxReplicaCount": int64(maxReplicas),
			"advanced": map[string]any{
				"horizontalPodAutoscalerConfig": map[string]any{
					"behavior": map[string]any{
						"scaleDown": map[string]any{
							"stabilizationWindowSeconds": int64(stabilizationS),
							"policies": []any{map[string]any{
								"type": "Pods", "value": int64(1), "periodSeconds": int64(300),
							}},
						},
					},
				},
			},
			"triggers": triggers,
		},
	)
}

// promTrigger is one KEDA Prometheus trigger. metricType Value, not the
// KEDA default AverageValue: the queries are already fleet aggregates
// (avg/max), and AverageValue would divide them by the replica count
// again — 0.9 average utilization across 2 pods reads as 0.45 and never
// crosses a 0.5 target. Found live.
func promTrigger(serverAddress, query, threshold string) map[string]any {
	return map[string]any{
		"type":       "prometheus",
		"metricType": "Value",
		"metadata": map[string]any{
			"serverAddress": serverAddress,
			"query":         query,
			"threshold":     threshold,
		},
	}
}

func toAnyMap(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
