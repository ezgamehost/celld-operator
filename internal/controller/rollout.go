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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
)

// The rollout controller (docs/celld-behaviors.md). Vanilla StatefulSet RollingUpdate
// gates each step on the NEW pod's readiness, but celld's documented gate is
// fleet-wide restoring=0 — and the restore work lands on the PEERS that
// absorbed the drained node's cells, not on the replacement pod. So the
// operator owns the partition counter and steps it one ordinal at a time,
// only when the whole fleet has re-warmed.

// minorVersion is the "major.minor" of a celld image tag, which is the
// granularity upstream's rolling-safety notes speak in.
type minorVersion struct{ major, minor int }

func (v minorVersion) String() string { return fmt.Sprintf("%d.%d", v.major, v.minor) }

func (v minorVersion) cmp(o minorVersion) int {
	if v.major != o.major {
		return v.major - o.major
	}
	return v.minor - o.minor
}

// versionBoundary is a line between two consecutive celld minors that a
// rollout must not cross as a rolling update in the flagged direction(s).
// A jump that skips minors still crosses every boundary in between.
type versionBoundary struct {
	lower, upper minorVersion
	// upward refuses lower -> upper (the mixed fleet breaks); downward
	// refuses upper -> lower (a downgrade upstream documents as lossy, or
	// the same mixed-fleet break in reverse).
	upward, downward bool
	reason           string
}

// breakingBoundaries is maintained from celld's release notes
// (docs/celld-behaviors.md, F8). Crossing one is refused unless the CR
// says Recreate; the reason is reported on the CR.
var breakingBoundaries = []versionBoundary{
	{
		lower: minorVersion{0, 1}, upper: minorVersion{0, 2}, upward: true, downward: true,
		reason: "v0.1 and v0.2 nodes cannot share a fleet (ownership records changed address " +
			"semantics and block objects changed format); upstream requires stop-all-then-start",
	},
	{
		// v0.2.1 -> v0.3.0 is rolling-safe upstream: a v0.3 node that
		// cannot replicate to a v0.2 peer falls back to bucket proofs. The
		// downgrade is not: a v0.2 binary cannot read writes still waiting
		// in v0.3's replicated log or bundle objects, so it can lose
		// acknowledged writes unless every node sealed its log on the way
		// out ("node-log close: sealed epoch" in the shutdown log).
		lower: minorVersion{0, 2}, upper: minorVersion{0, 3}, downward: true,
		reason: "a v0.2 node cannot read writes waiting in v0.3's replicated log, so the downgrade " +
			"can lose acknowledged writes unless every node's shutdown log shows " +
			"\"node-log close: sealed epoch\"",
	},
	{
		// v0.4 tunnels fetch, RPC, and WebSocket calls over a versioned
		// peer protocol that refuses v0.3 peers. Its large Workers KV value
		// references are also unreadable by v0.3, so neither direction may
		// run as a mixed fleet.
		lower: minorVersion{0, 3}, upper: minorVersion{0, 4}, upward: true, downward: true,
		reason: "v0.3 and v0.4 nodes cannot share a fleet (the peer tunnel protocols are " +
			"incompatible and v0.3 cannot read v0.4 large Workers KV value references); " +
			"upstream requires stop-all-then-start",
	},
}

// fleetOutcome is what one reconcile pass concluded about the fleet.
type fleetOutcome struct {
	Phase     platformv1alpha1.WorkerAppPhase
	Partition int32
	WaitingOn string
	Requeue   time.Duration
	// RolledOut reports that every pod serves spec.appVersion.
	RolledOut bool
}

func minorOf(image string) (minorVersion, bool) {
	image, _, _ = strings.Cut(image, "@")
	idx := strings.LastIndex(image, ":")
	if idx < 0 {
		return minorVersion{}, false
	}
	tag := strings.TrimPrefix(image[idx+1:], "v")
	parts := strings.SplitN(tag, ".", 3)
	if len(parts) < 2 {
		return minorVersion{}, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return minorVersion{}, false
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return minorVersion{}, false
	}
	return minorVersion{major, minor}, true
}

// breakingReason explains why moving between these celld images must not be
// a rolling update, or returns "" when no flagged boundary is crossed in a
// flagged direction. Unknown tags are treated as non-breaking: refusing
// every unparseable tag would block private builds, and the table is a
// guard for known hazards, not a proof of safety.
func breakingReason(fromImage, toImage string) string {
	from, okFrom := minorOf(fromImage)
	to, okTo := minorOf(toImage)
	if !okFrom || !okTo || from == to {
		return ""
	}
	var reasons []string
	for _, b := range breakingBoundaries {
		crossesUp := from.cmp(b.lower) <= 0 && to.cmp(b.upper) >= 0
		crossesDown := from.cmp(b.upper) >= 0 && to.cmp(b.lower) <= 0
		if (b.upward && crossesUp) || (b.downward && crossesDown) {
			reasons = append(reasons, b.reason)
		}
	}
	return strings.Join(reasons, "; ")
}

// isBreakingUpgrade reports whether moving between these celld images
// crosses a flagged boundary in a flagged direction.
func isBreakingUpgrade(fromImage, toImage string) bool {
	return breakingReason(fromImage, toImage) != ""
}

func stsContainerImage(sts *appsv1.StatefulSet) string {
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == celldContainerName {
			return c.Image
		}
	}
	return ""
}

func stsPartition(sts *appsv1.StatefulSet) int32 {
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
		return *ru.Partition
	}
	return 0
}

func setStsPartition(sts *appsv1.StatefulSet, partition int32) {
	sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
		Type: appsv1.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
			Partition: ptr.To(partition),
		},
	}
}

func autoscalingEnabled(app *platformv1alpha1.WorkerApp) bool {
	return app.Spec.Autoscaling != nil && app.Spec.Autoscaling.Enabled
}

// updateFleet writes the StatefulSet. A conflict is a normal race with the
// StatefulSet controller or the HPA, not an error: report it so the caller
// requeues shortly and re-decides from a fresh read.
func (r *WorkerAppReconciler) updateFleet(ctx context.Context, sts *appsv1.StatefulSet) (conflict bool, err error) {
	err = r.Update(ctx, sts)
	if apierrors.IsConflict(err) {
		return true, nil
	}
	return false, err
}

func conflictOutcome(app *platformv1alpha1.WorkerApp) fleetOutcome {
	phase := app.Status.Phase
	if phase == "" {
		phase = platformv1alpha1.PhasePending
	}
	return fleetOutcome{
		Phase:     phase,
		WaitingOn: "statefulset write conflicted; retrying",
		Requeue:   3 * time.Second,
	}
}

// configHash digests every Secret the pod template references, so a
// rotation changes the template and triggers a gated rollout. A missing
// Secret hashes as a distinct marker: its later creation also rolls the
// fleet. Secrets are read uncached (see main.go) — no cluster-wide Secret
// informer — so a rotation lands on the next reconcile rather than
// instantly; the steady-state requeue bounds that latency.
func (r *WorkerAppReconciler) configHash(ctx context.Context, app *platformv1alpha1.WorkerApp) (string, error) {
	var refs []string
	if app.Spec.Vars != nil && app.Spec.Vars.SecretRef != "" {
		refs = append(refs, app.Spec.Vars.SecretRef)
	}
	if ref := app.Spec.Bucket.CredentialsFrom.SecretRef; ref != "" {
		refs = append(refs, ref)
	}
	if len(refs) == 0 {
		return "", nil
	}
	digest := sha256.New()
	write := func(parts ...[]byte) {
		for _, p := range parts {
			_, _ = digest.Write(p)
			_, _ = digest.Write([]byte{0})
		}
	}
	for _, name := range refs {
		var secret corev1.Secret
		err := r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: name}, &secret)
		if apierrors.IsNotFound(err) {
			write([]byte("missing"), []byte(name))
			continue
		}
		if err != nil {
			return "", fmt.Errorf("reading secret %s: %w", name, err)
		}
		write([]byte("secret"), []byte(name))
		for _, k := range slices.Sorted(maps.Keys(secret.Data)) {
			write([]byte(k), secret.Data[k])
		}
	}
	return hex.EncodeToString(digest.Sum(nil))[:16], nil
}

// reconcileFleet drives the StatefulSet toward spec and returns the phase.
// appVersion is the resolved version (pinned or bucket-tracked).
func (r *WorkerAppReconciler) reconcileFleet(ctx context.Context, app *platformv1alpha1.WorkerApp, appVersion string) (fleetOutcome, error) {
	configHash, err := r.configHash(ctx, app)
	if err != nil {
		return fleetOutcome{}, err
	}
	desired := buildStatefulSet(app, configHash, appVersion)
	if err := ctrl.SetControllerReference(app, desired, r.Scheme); err != nil {
		return fleetOutcome{}, err
	}

	// Uncached read: every decision below (template change? partition
	// position?) must see the present, not the informer's recent past.
	var existing appsv1.StatefulSet
	err = r.Reader.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: fleetName(app)}, &existing)
	if apierrors.IsNotFound(err) {
		// Fresh fleet: no gating — pods come up ordered, and there is
		// nothing warm to protect yet.
		if err := r.Create(ctx, desired); err != nil {
			return fleetOutcome{}, err
		}
		return fleetOutcome{Phase: platformv1alpha1.PhasePending, Requeue: 10 * time.Second}, nil
	}
	if err != nil {
		return fleetOutcome{}, err
	}

	if err := requireOwnership(app, &existing); err != nil {
		return fleetOutcome{}, err
	}

	desiredHash := desired.Annotations[templateHashAnnotation]
	templateChanged := existing.Annotations[templateHashAnnotation] != desiredHash
	oldImage := stsContainerImage(&existing)
	imageChanged := oldImage != "" && oldImage != app.Spec.Celld.Image

	if templateChanged || stsPartition(&existing) > 0 || existing.Annotations[recreateAnnotation] == annotationTrue || (!autoscalingEnabled(app) && ptr.Deref(existing.Spec.Replicas, 0) != desiredReplicas(app)) {
		paused, err := r.pauseScaling(ctx, app)
		if err != nil {
			return fleetOutcome{}, err
		}
		if !paused {
			return fleetOutcome{Phase: platformv1alpha1.PhaseRollingOut, WaitingOn: "waiting for KEDA pause and HPA removal", Requeue: 5 * time.Second}, nil
		}
	}
	if refusal := runtimeChangeRefusal(app, oldImage); refusal != "" {
		return fleetOutcome{Phase: platformv1alpha1.PhaseDegraded, WaitingOn: refusal, Requeue: time.Minute}, nil
	}
	if existing.Annotations[recreateAnnotation] == annotationTrue {
		return r.recreateStep(ctx, app, &existing, desired, "")
	}
	if templateChanged {
		var breaking string
		if imageChanged {
			breaking = breakingReason(oldImage, app.Spec.Celld.Image)
		}
		if breaking != "" && app.Spec.Celld.UpdateStrategy != platformv1alpha1.UpdateStrategyRecreate {
			// Refuse: a rolling update across this boundary creates the
			// mixed fleet upstream forbids, or the lossy downgrade it
			// documents (F8). A GitOps diff alone must not be able to do
			// this; the CR has to say Recreate.
			return fleetOutcome{
				Phase: platformv1alpha1.PhaseDegraded,
				WaitingOn: fmt.Sprintf("celld %s -> %s is not rolling-safe: %s; set celld.updateStrategy: Recreate",
					oldImage, app.Spec.Celld.Image, breaking),
			}, nil
		}
		if app.Spec.Celld.UpdateStrategy == platformv1alpha1.UpdateStrategyRecreate {
			return r.recreateStep(ctx, app, &existing, desired, breaking)
		}
		// Start a gated rolling update: apply the new template frozen at
		// partition == live replicas, so no pod moves until we step.
		liveReplicas := ptr.Deref(existing.Spec.Replicas, 0)
		existing.Spec.Template = desired.Spec.Template
		if existing.Annotations == nil {
			existing.Annotations = map[string]string{}
		}
		existing.Annotations[templateHashAnnotation] = desiredHash
		existing.Annotations[settleAnnotation] = time.Now().Add(10 * time.Second).UTC().Format(time.RFC3339Nano)
		setStsPartition(&existing, liveReplicas)
		if conflict, err := r.updateFleet(ctx, &existing); err != nil {
			return fleetOutcome{}, err
		} else if conflict {
			return conflictOutcome(app), nil
		}
		return fleetOutcome{
			Phase:     platformv1alpha1.PhaseRollingOut,
			Partition: liveReplicas,
			Requeue:   10 * time.Second,
		}, nil
	}

	if partition := stsPartition(&existing); partition > 0 {
		return r.rolloutStep(ctx, app, &existing)
	}

	return r.steadyFleet(ctx, app, &existing, appVersion)
}

// rolloutStep advances a gated rolling update by at most one ordinal.
// Watch events can reconcile sooner than Requeue; the live gates below,
// rather than the requeue interval, decide whether another step is safe.
func (r *WorkerAppReconciler) rolloutStep(ctx context.Context, app *platformv1alpha1.WorkerApp, sts *appsv1.StatefulSet) (fleetOutcome, error) {
	partition := stsPartition(sts)
	liveReplicas := ptr.Deref(sts.Spec.Replicas, 0)
	if partition > liveReplicas {
		partition = liveReplicas
		setStsPartition(sts, partition)
		if conflict, err := r.updateFleet(ctx, sts); err != nil {
			return fleetOutcome{}, err
		} else if conflict {
			return conflictOutcome(app), nil
		}
	}
	out := fleetOutcome{Phase: platformv1alpha1.PhaseRollingOut, Partition: partition, Requeue: 10 * time.Second}

	if partition <= 0 || sts.Status.ObservedGeneration != sts.Generation {
		out.WaitingOn = "waiting for StatefulSet observation"
		return out, nil
	}
	if deadline, err := time.Parse(time.RFC3339Nano, sts.Annotations[settleAnnotation]); err == nil && time.Now().Before(deadline) {
		out.WaitingOn = "waiting for rollout settle interval"
		out.Requeue = time.Until(deadline)
		return out, nil
	}
	// Gate 1: every already-released ordinal (>= partition) runs the new
	// revision and is Ready.
	if sts.Status.UpdateRevision == "" {
		out.WaitingOn = "statefulset: update revision pending"
		return out, nil
	}
	for ord := partition; ord < liveReplicas; ord++ {
		name := fmt.Sprintf("%s-%d", fleetName(app), ord)
		var pod corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: name}, &pod); err != nil {
			out.WaitingOn = name + ": missing"
			return out, client.IgnoreNotFound(err)
		}
		if pod.Labels["controller-revision-hash"] != sts.Status.UpdateRevision {
			out.WaitingOn = name + ": updating"
			return out, nil
		}
		if !podReady(&pod) {
			out.WaitingOn = name + ": not ready"
			return out, nil
		}
	}

	// Require every expected ordinal, including old pods, to answer a valid
	// live state request. Recreate is the explicit recovery path for a fleet
	// too unhealthy to satisfy the rolling safety contract.
	_, restoring, err := r.observeFleet(ctx, app, sts)
	if err != nil || restoring != 0 {
		out.WaitingOn = fmt.Sprintf("fleet state: restoring=%d, error=%v", restoring, err)
		return out, nil
	}

	// Both gates pass: release the next ordinal.
	partition--
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[settleAnnotation] = time.Now().Add(10 * time.Second).UTC().Format(time.RFC3339Nano)
	setStsPartition(sts, partition)
	if conflict, err := r.updateFleet(ctx, sts); err != nil {
		return fleetOutcome{}, err
	} else if conflict {
		return conflictOutcome(app), nil
	}
	out.Partition = partition
	if partition == 0 {
		out.WaitingOn = fmt.Sprintf("%s-0: updating", fleetName(app))
	} else {
		out.WaitingOn = fmt.Sprintf("%s-%d: updating", fleetName(app), partition)
	}
	return out, nil
}

// recreateStep executes the stop-all-then-start path for celld version
// changes that forbid mixed fleets or a live downgrade (F8). An
// availability event by design; the CR had to say Recreate explicitly.
// hazard, when non-empty, is the upstream reason the change is not
// rolling-safe; it is surfaced while the fleet drains so an operator
// watching the CR sees what the stop is protecting.
func (r *WorkerAppReconciler) recreateStep(ctx context.Context, app *platformv1alpha1.WorkerApp, existing, desired *appsv1.StatefulSet, hazard string) (fleetOutcome, error) {
	out := fleetOutcome{Phase: platformv1alpha1.PhaseRecreating, Requeue: 10 * time.Second}

	// Phase A: scale the OLD template to zero and let every node drain.
	if ptr.Deref(existing.Spec.Replicas, 0) != 0 {
		existing.Spec.Replicas = ptr.To(int32(0))
		if existing.Annotations == nil {
			existing.Annotations = map[string]string{}
		}
		existing.Annotations[recreateAnnotation] = annotationTrue
		if conflict, err := r.updateFleet(ctx, existing); err != nil {
			return fleetOutcome{}, err
		} else if conflict {
			return conflictOutcome(app), nil
		}
		out.WaitingOn = "scaling to zero for non-rolling celld change"
		if hazard != "" {
			out.WaitingOn += ": " + hazard
		}
		return out, nil
	}
	var oldPods corev1.PodList
	if err := r.Reader.List(ctx, &oldPods, client.InNamespace(app.Namespace), client.MatchingLabels(selectorLabels(app))); err != nil {
		return out, err
	}
	if existing.Status.ObservedGeneration != existing.Generation || len(oldPods.Items) > 0 || existing.Status.Replicas > 0 {
		out.WaitingOn = fmt.Sprintf("%d pods draining", existing.Status.Replicas)
		return out, nil
	}

	// Phase B: the fleet is stopped; apply the new template and scale up.
	existing.Spec.Template = desired.Spec.Template
	if existing.Annotations == nil {
		existing.Annotations = map[string]string{}
	}
	existing.Annotations[templateHashAnnotation] = desired.Annotations[templateHashAnnotation]
	delete(existing.Annotations, recreateAnnotation)
	existing.Spec.Replicas = ptr.To(desiredReplicas(app))
	setStsPartition(existing, 0)
	if conflict, err := r.updateFleet(ctx, existing); err != nil {
		return fleetOutcome{}, err
	} else if conflict {
		return conflictOutcome(app), nil
	}
	out.WaitingOn = "starting new fleet"
	return out, nil
}

func podReady(pod *corev1.Pod) bool {
	if !pod.DeletionTimestamp.IsZero() {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

const settleAnnotation = "celld-operator.io/settle-until"
const recreateAnnotation = "celld-operator.io/recreating"

func (r *WorkerAppReconciler) observeFleet(ctx context.Context, app *platformv1alpha1.WorkerApp, sts *appsv1.StatefulSet) (map[string]*PodState, int64, error) {
	var pods corev1.PodList
	if err := r.Reader.List(ctx, &pods, client.InNamespace(app.Namespace), client.MatchingLabels(selectorLabels(app))); err != nil {
		return nil, 0, err
	}
	expected := ptr.Deref(sts.Spec.Replicas, 0)
	if expected < 1 || int32(len(pods.Items)) != expected {
		return nil, 0, fmt.Errorf("expected %d pods, found %d", expected, len(pods.Items))
	}
	names := map[string]bool{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, sts) || !podReady(pod) {
			return nil, 0, fmt.Errorf("pod %s is not ready or not owned by the fleet", pod.Name)
		}
		names[pod.Name] = true
	}
	for i := range expected {
		if !names[fmt.Sprintf("%s-%d", sts.Name, i)] {
			return nil, 0, fmt.Errorf("missing ordinal %d", i)
		}
	}
	return r.State.FleetSweep(ctx, pods.Items)
}

// KEDA acknowledges a full pause only after stopping its scale loop and
// deleting its HPA. Independently check that no HPA still targets this fleet.
func (r *WorkerAppReconciler) pauseScaling(ctx context.Context, app *platformv1alpha1.WorkerApp) (bool, error) {
	obj := newUnstructuredObject("keda.sh/v1alpha1", "ScaledObject", fleetName(app), app.Namespace, nil, nil, nil)
	err := r.Reader.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if !autoscalingEnabled(app) {
		if err == nil {
			var conditions []metav1.Condition
			if r.ensureScaledObject(ctx, app, fleetOutcome{}, &conditions) {
				return false, fmt.Errorf("could not remove disabled scaler")
			}
			return false, nil
		}
		if !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
			return false, err
		}
	} else {
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if apierrors.IsNotFound(err) || obj.GetAnnotations()[kedaPausedAnnotation] != annotationTrue {
			desired := buildScaledObject(app, r.PrometheusURL, true)
			return false, r.ensureUnstructured(ctx, app, desired)
		}
		if err := requireOwnership(app, obj); err != nil {
			return false, err
		}
		conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
		paused := false
		for _, item := range conditions {
			if cond, ok := item.(map[string]any); ok && cond["type"] == "Paused" && cond["status"] == "True" {
				paused = true
			}
		}
		if !paused {
			return false, nil
		}
	}
	var hpas autoscalingv2.HorizontalPodAutoscalerList
	if err := r.Reader.List(ctx, &hpas, client.InNamespace(app.Namespace)); err != nil {
		return false, err
	}
	for _, hpa := range hpas.Items {
		if hpa.Spec.ScaleTargetRef.Kind == "StatefulSet" && hpa.Spec.ScaleTargetRef.Name == fleetName(app) {
			return false, nil
		}
	}
	return true, nil
}

func (r *WorkerAppReconciler) steadyFleet(ctx context.Context, app *platformv1alpha1.WorkerApp, existing *appsv1.StatefulSet, appVersion string) (fleetOutcome, error) {
	// Steady state. Scale is ours only when KEDA does not own it.
	if !autoscalingEnabled(app) && ptr.Deref(existing.Spec.Replicas, 0) != desiredReplicas(app) {
		existing.Spec.Replicas = ptr.To(desiredReplicas(app))
		if conflict, err := r.updateFleet(ctx, existing); err != nil {
			return fleetOutcome{}, err
		} else if conflict {
			return conflictOutcome(app), nil
		}
		return fleetOutcome{Phase: platformv1alpha1.PhasePending, Requeue: 10 * time.Second}, nil
	}

	liveReplicas := ptr.Deref(existing.Spec.Replicas, 0)
	converged := existing.Status.ReadyReplicas == liveReplicas &&
		existing.Status.UpdatedReplicas == liveReplicas &&
		existing.Status.ObservedGeneration == existing.Generation
	if !converged {
		return fleetOutcome{
			Phase:     platformv1alpha1.PhasePending,
			WaitingOn: fmt.Sprintf("fleet: %d/%d ready", existing.Status.ReadyReplicas, liveReplicas),
			Requeue:   15 * time.Second,
		}, nil
	}
	states, restoring, err := r.observeFleet(ctx, app, existing)
	if err != nil || restoring != 0 {
		return fleetOutcome{Phase: platformv1alpha1.PhasePending, WaitingOn: fmt.Sprintf("waiting for complete fleet state: restoring=%d, error=%v", restoring, err), Requeue: 15 * time.Second}, nil
	}
	for name, state := range states {
		if state.Deployment == nil || state.Deployment.Version != appVersion || len(state.Deployment.Draining) > 0 || state.Deployment.Swapping != 0 {
			return fleetOutcome{Phase: platformv1alpha1.PhasePending, WaitingOn: name + ": deployment not converged", Requeue: 15 * time.Second}, nil
		}
	}
	return fleetOutcome{Phase: platformv1alpha1.PhaseReady, RolledOut: true, Requeue: 30 * time.Second}, nil
}

func runtimeChangeRefusal(app *platformv1alpha1.WorkerApp, oldImage string) string {
	from, fromOK := minorOf(oldImage)
	to, toOK := minorOf(app.Spec.Celld.Image)
	if fromOK && toOK && from.cmp(minorVersion{0, 3}) >= 0 && to.cmp(minorVersion{0, 3}) < 0 {
		return "downgrade requires offline durability verification and migration to a new WorkerApp"
	}
	if oldImage != app.Spec.Celld.Image && (!fromOK || !toOK) && app.Spec.Celld.UpdateStrategy != platformv1alpha1.UpdateStrategyRecreate {
		return "unrecognized live image requires Recreate"
	}
	return ""
}
