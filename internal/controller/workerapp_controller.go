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
	"context"
	"fmt"
	"maps"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
)

// Condition types reported on WorkerApp status.
const (
	condBucketCredentialsReady = "BucketCredentialsReady"
	condIngressReady           = "IngressReady"
	condMeshPolicyReady        = "MeshPolicyReady"
	condAutoscalingReady       = "AutoscalingReady"
	condDeployTrackingReady    = "DeployTrackingReady"

	reasonRouteError = "RouteError"
)

// Ingress modes (--ingress-mode). HTTPRoute is the Gateway API path from
// docs/celld-behaviors.md; VirtualService targets clusters whose ingress is an
// existing classic istio-ingressgateway (Gateway API CRDs on the standard
// channel drop the retry field, and older Istio releases do not attach
// Gateway API Gateways to pre-existing deployments).
const (
	IngressModeHTTPRoute      = "httproute"
	IngressModeVirtualService = "virtualservice"
	// IngressModeIngress emits networking.k8s.io/v1 Ingress objects, for
	// clusters fronted by a classic ingress controller (ingress-nginx,
	// Traefik, cloud LB controllers).
	IngressModeIngress = "ingress"
	IngressModeNone    = "none"
)

// WorkerAppReconciler reconciles one celld fleet per WorkerApp: the
// StatefulSet and its rollout, both Services, the network and mesh policy
// around the unauthenticated internal listener, the PDB, the HTTPRoute on
// the shared Gateway, and the KEDA ScaledObject (docs/celld-behaviors.md).
type WorkerAppReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	State  *StateClient

	// Reader reads uncached. The rollout state machine's StatefulSet reads
	// go through it: a stale informer view mid-rollout re-enters the
	// template-change branch and resets partition progress (observed live),
	// and stale resourceVersions turn every update into a conflict.
	Reader client.Reader

	// Deploys caches per-fleet reads of the bucket's deploy/current.json
	// for appVersion "auto" tracking and pinned-mode drift warnings.
	Deploys *DeployTracker

	// IngressMode selects how hostnames are routed: httproute (default),
	// virtualservice, or none.
	IngressMode string
	// The shared edge Gateway that HTTPRoutes attach to (httproute mode).
	GatewayName      string
	GatewayNamespace string
	// IstioGateways are the pre-existing networking.istio.io Gateways that
	// VirtualServices bind to (virtualservice mode), as "namespace/name".
	IstioGateways []string
	// IngressClassName selects the controller in ingress mode; empty uses
	// the cluster default IngressClass.
	IngressClassName string
	// ClusterIssuer, when set, adds the cert-manager annotation and a TLS
	// block to emitted Ingresses so each app gets a certificate.
	ClusterIssuer string
	// PrometheusURL is where KEDA reads the operator's exported metrics.
	PrometheusURL string
	// OperatorNamespace is allowed by NetworkPolicy to reach :8081.
	OperatorNamespace string
	// OperatorPrincipal is the operator's SPIFFE-style identity for the
	// Istio AuthorizationPolicy.
	OperatorPrincipal string
}

// +kubebuilder:rbac:groups=celld-operator.io,resources=workerapps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=celld-operator.io,resources=workerapps/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=celld-operator.io,resources=workerapps/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies;ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=security.istio.io,resources=authorizationpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=virtualservices,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives one WorkerApp toward its spec.
func (r *WorkerAppReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	app := &platformv1alpha1.WorkerApp{}
	if err := r.Get(ctx, req.NamespacedName, app); err != nil {
		// Deleted: children are owned and garbage-collected.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var conditions []metav1.Condition

	// Foundation objects first: the StatefulSet references the service
	// account and headless service by name.
	if err := r.ensureFoundation(ctx, app, &conditions); err != nil {
		return ctrl.Result{}, err
	}

	// Resolve which application version the fleet should serve — the
	// pinned spec value, or the bucket's deploy pointer in auto mode —
	// then run the fleet's gated rollout state machine against it.
	appVersion, tracking := r.resolveAppVersion(ctx, app, &conditions)
	outcome, err := r.reconcileFleet(ctx, app, appVersion)
	if err != nil {
		return ctrl.Result{}, err
	}
	if tracking && (outcome.Requeue == 0 || outcome.Requeue > r.Deploys.Interval) {
		// Auto mode notices a new `celld deploy` within one poll interval.
		outcome.Requeue = r.Deploys.Interval
	}

	// Edge, mesh, and autoscaling; each tolerates its CRD being absent so
	// the operator runs on clusters without Gateway API, Istio, or KEDA and
	// says so in conditions instead of failing the fleet. A transient
	// failure (a conflict with another controller, an apiserver blip) is
	// retried quickly — waiting out the steady-state requeue leaves a
	// broken route or scaler in place for minutes.
	transient := r.ensureIngress(ctx, app, &conditions)
	transient = r.ensureAuthorizationPolicy(ctx, app, &conditions) || transient
	transient = r.ensureScaledObject(ctx, app, outcome, &conditions) || transient
	if transient && (outcome.Requeue == 0 || outcome.Requeue > 15*time.Second) {
		outcome.Requeue = 15 * time.Second
	}

	// Status: live fleet numbers plus the rollout position.
	if err := r.updateStatus(ctx, app, outcome, appVersion, conditions); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	if outcome.WaitingOn != "" {
		log.Info("fleet reconciled", "phase", outcome.Phase, "waitingOn", outcome.WaitingOn)
	}
	return ctrl.Result{RequeueAfter: outcome.Requeue}, nil
}

func (r *WorkerAppReconciler) ensureFoundation(ctx context.Context, app *platformv1alpha1.WorkerApp, conditions *[]metav1.Condition) error {
	sa := buildServiceAccount(app)
	if err := r.ensureObject(ctx, app, sa, func(live, desired client.Object) {
		live.SetAnnotations(desired.GetAnnotations())
	}); err != nil {
		return fmt.Errorf("service account: %w", err)
	}
	if app.Spec.Bucket.CredentialsFrom.IAMRole == iamRoleAuto {
		// docs/celld-behaviors.md "known not-implemented": automatic IAM provisioning is not
		// built yet. The fleet still runs; credentials must arrive by
		// annotating the fleet ServiceAccount (or via secretRef).
		*conditions = append(*conditions, metav1.Condition{
			Type: condBucketCredentialsReady, Status: metav1.ConditionFalse,
			Reason:  "ProvisioningNotImplemented",
			Message: fmt.Sprintf("iamRole: auto is not implemented; annotate ServiceAccount %s with the role for prefix %s", fleetName(app), app.Spec.Bucket.Name),
		})
	} else {
		*conditions = append(*conditions, metav1.Condition{
			Type: condBucketCredentialsReady, Status: metav1.ConditionTrue, Reason: "Configured",
		})
	}

	internal := buildInternalService(app)
	if err := r.ensureObject(ctx, app, internal, func(live, desired client.Object) {
		l, d := live.(*corev1.Service), desired.(*corev1.Service)
		l.Spec.Selector = d.Spec.Selector
		l.Spec.Ports = d.Spec.Ports
		l.Spec.PublishNotReadyAddresses = d.Spec.PublishNotReadyAddresses
	}); err != nil {
		return fmt.Errorf("internal service: %w", err)
	}

	public := buildPublicService(app)
	if err := r.ensureObject(ctx, app, public, func(live, desired client.Object) {
		l, d := live.(*corev1.Service), desired.(*corev1.Service)
		l.Spec.Selector = d.Spec.Selector
		l.Spec.Ports = d.Spec.Ports
		l.Spec.Type = d.Spec.Type
		// Cloud LB configuration rides on annotations; merge so the cloud
		// controller's own bookkeeping annotations survive.
		l.SetAnnotations(mergeStringMaps(l.GetAnnotations(), d.GetAnnotations()))
	}); err != nil {
		return fmt.Errorf("public service: %w", err)
	}

	netpol := buildNetworkPolicy(app, r.OperatorNamespace)
	if err := r.ensureObject(ctx, app, netpol, func(live, desired client.Object) {
		l, d := live.(*networkingv1.NetworkPolicy), desired.(*networkingv1.NetworkPolicy)
		l.Spec = d.Spec
	}); err != nil {
		return fmt.Errorf("network policy: %w", err)
	}

	pdb := buildPDB(app)
	if err := r.ensureObject(ctx, app, pdb, func(live, desired client.Object) {
		l, d := live.(*policyv1.PodDisruptionBudget), desired.(*policyv1.PodDisruptionBudget)
		l.Spec.MaxUnavailable = d.Spec.MaxUnavailable
		l.Spec.Selector = d.Spec.Selector
	}); err != nil {
		return fmt.Errorf("pod disruption budget: %w", err)
	}
	return nil
}

// ensureObject creates the object or applies the desired mutation to the
// live copy. The mutation copies only fields this operator owns, so server
// defaulting and other controllers' fields survive. An update that would
// change nothing is skipped — a fleet reconcile touches half a dozen
// objects, and unconditional writes amplify every pod-churn burst into an
// apiserver write storm.
func (r *WorkerAppReconciler) ensureObject(ctx context.Context, app *platformv1alpha1.WorkerApp, desired client.Object, mutate func(live, desired client.Object)) error {
	if err := ctrl.SetControllerReference(app, desired, r.Scheme); err != nil {
		return err
	}
	live := desired.DeepCopyObject().(client.Object)
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), live)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	before := live.DeepCopyObject().(client.Object)
	mutate(live, desired)
	live.SetLabels(mergeStringMaps(live.GetLabels(), desired.GetLabels()))
	if apiequality.Semantic.DeepEqual(before, live) {
		return nil
	}
	return r.Update(ctx, live)
}

// The ensure helpers return true when they hit a transient error worth a
// fast requeue (anything but a missing CRD, which only changes when a
// human installs something).
func (r *WorkerAppReconciler) ensureIngress(ctx context.Context, app *platformv1alpha1.WorkerApp, conditions *[]metav1.Condition) bool {
	if len(app.Spec.Hostnames) == 0 {
		return false
	}
	switch r.IngressMode {
	case IngressModeNone:
		return false
	case IngressModeVirtualService:
		return r.ensureVirtualService(ctx, app, conditions)
	case IngressModeIngress:
		return r.ensureV1Ingress(ctx, app, conditions)
	default:
		return r.ensureHTTPRoute(ctx, app, conditions)
	}
}

func (r *WorkerAppReconciler) ensureV1Ingress(ctx context.Context, app *platformv1alpha1.WorkerApp, conditions *[]metav1.Condition) bool {
	ingress := buildIngress(app, r.IngressClassName, r.ClusterIssuer)
	err := r.ensureObject(ctx, app, ingress, func(live, desired client.Object) {
		l, d := live.(*networkingv1.Ingress), desired.(*networkingv1.Ingress)
		l.Spec = d.Spec
		// Annotations carry the route policy; merge so other controllers'
		// bookkeeping survives.
		l.SetAnnotations(mergeStringMaps(l.GetAnnotations(), d.GetAnnotations()))
	})
	if err != nil {
		*conditions = append(*conditions, metav1.Condition{
			Type: condIngressReady, Status: metav1.ConditionFalse,
			Reason: reasonRouteError, Message: err.Error(),
		})
		return true
	}
	*conditions = append(*conditions, metav1.Condition{
		Type: condIngressReady, Status: metav1.ConditionTrue, Reason: "IngressReconciled",
	})
	return false
}

func (r *WorkerAppReconciler) ensureVirtualService(ctx context.Context, app *platformv1alpha1.WorkerApp, conditions *[]metav1.Condition) bool {
	vs := buildVirtualService(app, r.IstioGateways)
	err := r.ensureUnstructured(ctx, app, vs)
	switch {
	case err == nil:
		*conditions = append(*conditions, metav1.Condition{
			Type: condIngressReady, Status: metav1.ConditionTrue, Reason: "VirtualServiceReconciled",
		})
	case meta.IsNoMatchError(err):
		*conditions = append(*conditions, metav1.Condition{
			Type: condIngressReady, Status: metav1.ConditionFalse,
			Reason:  "IstioUnavailable",
			Message: "networking.istio.io CRDs are not installed; hostnames are not routed",
		})
	default:
		*conditions = append(*conditions, metav1.Condition{
			Type: condIngressReady, Status: metav1.ConditionFalse,
			Reason: reasonRouteError, Message: err.Error(),
		})
		return true
	}
	return false
}

func (r *WorkerAppReconciler) ensureHTTPRoute(ctx context.Context, app *platformv1alpha1.WorkerApp, conditions *[]metav1.Condition) bool {
	route := buildHTTPRoute(app, r.GatewayName, r.GatewayNamespace)
	err := r.ensureObject(ctx, app, route, func(live, desired client.Object) {
		l, d := live.(*gatewayv1.HTTPRoute), desired.(*gatewayv1.HTTPRoute)
		l.Spec = d.Spec
	})
	switch {
	case err == nil:
		// Standard-channel Gateway API CRDs silently drop the experimental
		// retry field; per the fail-loud rule, say so rather than let the
		// drain-503 retry policy vanish quietly.
		if dropped, checkErr := r.httpRouteRetryDropped(ctx, route); checkErr == nil && dropped {
			*conditions = append(*conditions, metav1.Condition{
				Type: condIngressReady, Status: metav1.ConditionTrue,
				Reason:  "RouteReconciledRetryDropped",
				Message: "cluster Gateway API CRDs dropped the retry field (standard channel); drain 503s are not retried at the gateway",
			})
			return false
		}
		*conditions = append(*conditions, metav1.Condition{
			Type: condIngressReady, Status: metav1.ConditionTrue, Reason: "RouteReconciled",
		})
	case meta.IsNoMatchError(err):
		*conditions = append(*conditions, metav1.Condition{
			Type: condIngressReady, Status: metav1.ConditionFalse,
			Reason:  "GatewayAPIUnavailable",
			Message: "gateway.networking.k8s.io CRDs are not installed; hostnames are not routed",
		})
	default:
		*conditions = append(*conditions, metav1.Condition{
			Type: condIngressReady, Status: metav1.ConditionFalse,
			Reason: reasonRouteError, Message: err.Error(),
		})
		return true
	}
	return false
}

func (r *WorkerAppReconciler) httpRouteRetryDropped(ctx context.Context, desired *gatewayv1.HTTPRoute) (bool, error) {
	if len(desired.Spec.Rules) == 0 || desired.Spec.Rules[0].Retry == nil {
		return false, nil
	}
	var live gatewayv1.HTTPRoute
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), &live); err != nil {
		return false, err
	}
	return len(live.Spec.Rules) > 0 && live.Spec.Rules[0].Retry == nil, nil
}

func (r *WorkerAppReconciler) ensureAuthorizationPolicy(ctx context.Context, app *platformv1alpha1.WorkerApp, conditions *[]metav1.Condition) bool {
	policy := buildAuthorizationPolicy(app, r.OperatorPrincipal)
	err := r.ensureUnstructured(ctx, app, policy)
	switch {
	case err == nil:
		*conditions = append(*conditions, metav1.Condition{
			Type: condMeshPolicyReady, Status: metav1.ConditionTrue, Reason: "PolicyReconciled",
		})
	case meta.IsNoMatchError(err):
		// No Istio: NetworkPolicy still guards :8081; the mesh layer is
		// defense in depth, not a requirement (docs/celld-behaviors.md).
		*conditions = append(*conditions, metav1.Condition{
			Type: condMeshPolicyReady, Status: metav1.ConditionFalse,
			Reason:  "IstioUnavailable",
			Message: "security.istio.io CRDs are not installed; NetworkPolicy alone guards the internal listener",
		})
	default:
		*conditions = append(*conditions, metav1.Condition{
			Type: condMeshPolicyReady, Status: metav1.ConditionFalse,
			Reason: "PolicyError", Message: err.Error(),
		})
		return true
	}
	return false
}

func (r *WorkerAppReconciler) ensureScaledObject(ctx context.Context, app *platformv1alpha1.WorkerApp, outcome fleetOutcome, conditions *[]metav1.Condition) bool {
	if !autoscalingEnabled(app) {
		// Best-effort cleanup if autoscaling was turned off.
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion("keda.sh/v1alpha1")
		obj.SetKind("ScaledObject")
		obj.SetName(fleetName(app))
		obj.SetNamespace(app.Namespace)
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
			logf.FromContext(ctx).Error(err, "deleting stale ScaledObject")
		}
		return false
	}
	// Paused whenever the fleet is not steady, so KEDA and the rollout
	// controller never fight over replica count (docs/celld-behaviors.md).
	paused := outcome.Phase != platformv1alpha1.PhaseReady
	scaled := buildScaledObject(app, r.PrometheusURL, paused)
	err := r.ensureUnstructured(ctx, app, scaled)
	switch {
	case err == nil:
		*conditions = append(*conditions, metav1.Condition{
			Type: condAutoscalingReady, Status: metav1.ConditionTrue, Reason: "ScaledObjectReconciled",
		})
	case meta.IsNoMatchError(err):
		*conditions = append(*conditions, metav1.Condition{
			Type: condAutoscalingReady, Status: metav1.ConditionFalse,
			Reason:  "KEDAUnavailable",
			Message: "keda.sh CRDs are not installed; spec.autoscaling has no effect",
		})
	default:
		*conditions = append(*conditions, metav1.Condition{
			Type: condAutoscalingReady, Status: metav1.ConditionFalse,
			Reason: "ScaledObjectError", Message: err.Error(),
		})
		return true
	}
	return false
}

func (r *WorkerAppReconciler) ensureUnstructured(ctx context.Context, app *platformv1alpha1.WorkerApp, desired *unstructured.Unstructured) error {
	if err := ctrl.SetControllerReference(app, desired, r.Scheme); err != nil {
		return err
	}
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(desired.GroupVersionKind())
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), live)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	// Merge, never replace, metadata — other controllers annotate and
	// label these objects (KEDA stamps ScaledObjects with a name label),
	// and wiping their keys puts both controllers in a conflict loop.
	// Skip the update entirely when our spec and metadata are already in
	// place, so a steady-state reconcile writes nothing.
	if reflect.DeepEqual(live.Object["spec"], desired.Object["spec"]) &&
		stringMapSubset(desired.GetLabels(), live.GetLabels()) &&
		stringMapSubset(desired.GetAnnotations(), live.GetAnnotations()) {
		return nil
	}
	live.Object["spec"] = desired.Object["spec"]
	live.SetLabels(mergeStringMaps(live.GetLabels(), desired.GetLabels()))
	live.SetAnnotations(mergeStringMaps(live.GetAnnotations(), desired.GetAnnotations()))
	return r.Update(ctx, live)
}

// mergeStringMaps overlays desired onto live: our keys win, foreign keys
// survive.
func mergeStringMaps(live, desired map[string]string) map[string]string {
	out := make(map[string]string, len(live)+len(desired))
	maps.Copy(out, live)
	maps.Copy(out, desired)
	return out
}

func stringMapSubset(sub, of map[string]string) bool {
	for k, v := range sub {
		if of[k] != v {
			return false
		}
	}
	return true
}

func (r *WorkerAppReconciler) updateStatus(ctx context.Context, app *platformv1alpha1.WorkerApp, outcome fleetOutcome, appVersion string, conditions []metav1.Condition) error {
	app.Status.Phase = outcome.Phase
	app.Status.Rollout = platformv1alpha1.RolloutStatus{
		Partition: outcome.Partition,
		WaitingOn: outcome.WaitingOn,
	}
	if outcome.RolledOut {
		app.Status.RolledOutAppVersion = appVersion
	}

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKey{Namespace: app.Namespace, Name: fleetName(app)}, &sts); err == nil {
		app.Status.Fleet.Ready = sts.Status.ReadyReplicas
	}
	// Fleet restoring is best-effort in status: a partial sweep (fleet mid-
	// change, pod restarting) reports what answered. The rollout gate does
	// its own strict sweep.
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(app.Namespace), client.MatchingLabels(selectorLabels(app))); err == nil {
		if _, restoring, err := r.State.FleetSweep(ctx, pods.Items); err == nil {
			app.Status.Fleet.Restoring = int32(restoring)
		}
	}

	progressing := outcome.Phase == platformv1alpha1.PhasePending ||
		outcome.Phase == platformv1alpha1.PhaseRollingOut ||
		outcome.Phase == platformv1alpha1.PhaseRecreating
	conditions = append(conditions,
		metav1.Condition{
			Type:   "Available",
			Status: boolToCondition(outcome.Phase == platformv1alpha1.PhaseReady),
			Reason: string(outcome.Phase),
		},
		metav1.Condition{
			Type:    "Progressing",
			Status:  boolToCondition(progressing),
			Reason:  string(outcome.Phase),
			Message: outcome.WaitingOn,
		},
		metav1.Condition{
			Type:    "Degraded",
			Status:  boolToCondition(outcome.Phase == platformv1alpha1.PhaseDegraded),
			Reason:  string(outcome.Phase),
			Message: outcome.WaitingOn,
		},
	)
	for _, cond := range conditions {
		cond.ObservedGeneration = app.Generation
		meta.SetStatusCondition(&app.Status.Conditions, cond)
	}
	return r.Status().Update(ctx, app)
}

func boolToCondition(b bool) metav1.ConditionStatus {
	if b {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// SetupWithManager sets up the controller with the Manager. HTTPRoute,
// ScaledObject, and AuthorizationPolicy are deliberately not watched: their
// CRDs may be absent, and a missing informer would fail the whole manager.
func (r *WorkerAppReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.State == nil {
		r.State = NewStateClient()
	}
	if r.Reader == nil {
		r.Reader = mgr.GetAPIReader()
	}
	if r.Deploys == nil {
		r.Deploys = NewDeployTracker(0)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.WorkerApp{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("workerapp").
		Complete(r)
}
