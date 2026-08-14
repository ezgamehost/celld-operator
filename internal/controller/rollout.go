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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
)

// The rollout controller (DESIGN.md §8). Vanilla StatefulSet RollingUpdate
// gates each step on the NEW pod's readiness, but celld's documented gate is
// fleet-wide restoring=0 — and the restore work lands on the PEERS that
// absorbed the drained node's cells, not on the replacement pod. So the
// operator owns the partition counter and steps it one ordinal at a time,
// only when the whole fleet has re-warmed.

// breakingBoundaries lists celld upgrades that upstream forbids as rolling
// (mixed fleets break). Maintained from celld release notes; DESIGN.md §13
// open question 1 tracks automating this. Entries are "major.minor" pairs.
var breakingBoundaries = [][2]string{
	// v0.1 -> v0.2: ownership records changed address semantics and block
	// objects changed format; upstream requires stop-all-then-start.
	{"0.1", "0.2"},
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

func minorOf(image string) (string, bool) {
	idx := strings.LastIndex(image, ":")
	if idx < 0 {
		return "", false
	}
	tag := strings.TrimPrefix(image[idx+1:], "v")
	parts := strings.SplitN(tag, ".", 3)
	if len(parts) < 2 {
		return "", false
	}
	if _, err := strconv.Atoi(parts[0]); err != nil {
		return "", false
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return "", false
	}
	return parts[0] + "." + parts[1], true
}

// isBreakingUpgrade reports whether moving between these celld images (in
// either direction) crosses a boundary that forbids a mixed fleet. Unknown
// tags are treated as non-breaking: refusing every unparseable tag would
// block private builds, and the table is a guard for known hazards, not a
// proof of safety.
func isBreakingUpgrade(fromImage, toImage string) bool {
	from, okFrom := minorOf(fromImage)
	to, okTo := minorOf(toImage)
	if !okFrom || !okTo || from == to {
		return false
	}
	for _, b := range breakingBoundaries {
		if (from == b[0] && to == b[1]) || (from == b[1] && to == b[0]) {
			return true
		}
	}
	return false
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
func (r *WorkerAppReconciler) reconcileFleet(ctx context.Context, app *platformv1alpha1.WorkerApp) (fleetOutcome, error) {
	configHash, err := r.configHash(ctx, app)
	if err != nil {
		return fleetOutcome{}, err
	}
	desired := buildStatefulSet(app, configHash)
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

	desiredHash := desired.Annotations[templateHashAnnotation]
	templateChanged := existing.Annotations[templateHashAnnotation] != desiredHash
	oldImage := stsContainerImage(&existing)
	imageChanged := oldImage != "" && oldImage != app.Spec.Celld.Image

	if templateChanged {
		if imageChanged && isBreakingUpgrade(oldImage, app.Spec.Celld.Image) &&
			app.Spec.Celld.UpdateStrategy != platformv1alpha1.UpdateStrategyRecreate {
			// Refuse: a rolling update across this boundary creates the
			// mixed fleet upstream forbids (F8). A GitOps diff alone must
			// not be able to do this; the CR has to say Recreate.
			return fleetOutcome{
				Phase:     platformv1alpha1.PhaseDegraded,
				WaitingOn: fmt.Sprintf("celld %s -> %s is not rolling-safe; set celld.updateStrategy: Recreate", oldImage, app.Spec.Celld.Image),
			}, nil
		}
		if imageChanged && app.Spec.Celld.UpdateStrategy == platformv1alpha1.UpdateStrategyRecreate {
			return r.recreateStep(ctx, app, &existing, desired)
		}
		// Start a gated rolling update: apply the new template frozen at
		// partition == live replicas, so no pod moves until we step.
		liveReplicas := ptr.Deref(existing.Spec.Replicas, 0)
		existing.Spec.Template = desired.Spec.Template
		if existing.Annotations == nil {
			existing.Annotations = map[string]string{}
		}
		existing.Annotations[templateHashAnnotation] = desiredHash
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

	// Steady state. Scale is ours only when KEDA does not own it.
	if !autoscalingEnabled(app) && ptr.Deref(existing.Spec.Replicas, 0) != desiredReplicas(app) {
		existing.Spec.Replicas = ptr.To(desiredReplicas(app))
		if conflict, err := r.updateFleet(ctx, &existing); err != nil {
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
	return fleetOutcome{Phase: platformv1alpha1.PhaseReady, RolledOut: true, Requeue: 5 * time.Minute}, nil
}

// rolloutStep advances a gated rolling update by at most one ordinal.
// Reconciles arrive at least Requeue apart, so steps are naturally paced —
// the damper DESIGN.md §8 calls settle time.
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

	// Gate 2: fleet-wide restoring == 0, from a live sweep of every pod's
	// internal /state. The cold work lands on the peers, so the whole fleet
	// is polled. A pod that is Ready but unreachable holds the gate — never
	// step on missing data. A pod that is unreachable AND not Ready holds
	// no cells (celld is not serving) and is skipped: otherwise a fleet
	// that was never healthy could never roll out the fix for what broke
	// it, and the gate becomes a deadlock.
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(app.Namespace), client.MatchingLabels(selectorLabels(app))); err != nil {
		return fleetOutcome{}, err
	}
	var restoring int64
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		state, err := r.State.Fetch(ctx, pod.Status.PodIP)
		if err != nil {
			if podReady(pod) {
				out.WaitingOn = fmt.Sprintf("state unreachable: pod %s: %v", pod.Name, err)
				out.Requeue = 15 * time.Second
				return out, nil
			}
			continue
		}
		restoring += state.Restoring
	}
	if restoring > 0 {
		out.WaitingOn = fmt.Sprintf("fleet: restoring=%d", restoring)
		out.Requeue = 15 * time.Second
		return out, nil
	}

	// Both gates pass: release the next ordinal.
	partition--
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

// recreateStep executes the stop-all-then-start path for celld upgrades
// that forbid mixed fleets (F8). An availability event by design; the CR
// had to say Recreate explicitly.
func (r *WorkerAppReconciler) recreateStep(ctx context.Context, app *platformv1alpha1.WorkerApp, existing, desired *appsv1.StatefulSet) (fleetOutcome, error) {
	out := fleetOutcome{Phase: platformv1alpha1.PhaseRecreating, Requeue: 10 * time.Second}

	// Phase A: scale the OLD template to zero and let every node drain.
	if ptr.Deref(existing.Spec.Replicas, 0) != 0 {
		existing.Spec.Replicas = ptr.To(int32(0))
		if conflict, err := r.updateFleet(ctx, existing); err != nil {
			return fleetOutcome{}, err
		} else if conflict {
			return conflictOutcome(app), nil
		}
		out.WaitingOn = "scaling to zero for non-rolling celld upgrade"
		return out, nil
	}
	if existing.Status.Replicas > 0 {
		out.WaitingOn = fmt.Sprintf("%d pods draining", existing.Status.Replicas)
		return out, nil
	}

	// Phase B: the fleet is stopped; apply the new template and scale up.
	existing.Spec.Template = desired.Spec.Template
	if existing.Annotations == nil {
		existing.Annotations = map[string]string{}
	}
	existing.Annotations[templateHashAnnotation] = desired.Annotations[templateHashAnnotation]
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
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
