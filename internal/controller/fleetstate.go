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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
)

// celld ships no metrics endpoint (docs/celld-behaviors.md F9). The operator polls each
// fleet pod's internal /state — it is the one authorized cross-namespace
// caller — and re-exports what it sees as Prometheus metrics. The same
// series drive KEDA autoscaling, dashboards, and alerting; the rollout
// controller does its own live sweep at gate time rather than trusting this
// cache.

const (
	labelNamespace = "namespace"
	labelWorkerApp = "workerapp"
	labelPod       = "pod"
)

// A collector publishes immutable snapshots so a scrape cannot observe half
// of a sweep. Replacing the snapshot also removes deleted pod/fleet series.
type stateSnapshot struct {
	mu      sync.RWMutex
	samples []prometheus.Metric
}

var fleetMetrics = &stateSnapshot{}

func (s *stateSnapshot) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(s, ch) }
func (s *stateSnapshot) Collect(ch chan<- prometheus.Metric) {
	s.mu.RLock()
	samples := s.samples
	s.mu.RUnlock()
	for _, sample := range samples {
		ch <- sample
	}
}
func (s *stateSnapshot) publish(samples []prometheus.Metric) {
	s.mu.Lock()
	s.samples = samples
	s.mu.Unlock()
}
func init() { metrics.Registry.MustRegister(fleetMetrics) }

const selfFenceExitCode = 3

// DeploymentState reports the runtime deployment currently observed on a node.
type DeploymentState struct {
	Version    string            `json:"version"`
	Generation int64             `json:"generation"`
	Draining   []json.RawMessage `json:"draining"`
	Swapping   int64             `json:"swapping"`
}

// PodState is one /state sample. Safety counters are required; optional
// diagnostic fields remain compatible with older response schemas.
type PodState struct {
	Deployment *DeploymentState `json:"deployment"`

	Occupied int64 `json:"occupied"`
	Evicting int64 `json:"evicting"`
	// Restoring is state_json's activation backlog: every cold route that
	// holds an activation permit or waits for one. v0.3.0 also reports
	// its parts.
	Restoring         int64 `json:"restoring"`
	Activating        int64 `json:"activating"`
	ActivationWaiting int64 `json:"activation_waiting"`
	CapacityWaiting   int64 `json:"capacity_waiting"`
	// Shedding is null while healthy and a reason string during pressure
	// shedding ("memory": the cells' memory or active cgroup working set
	// crossed CELLD_MAX_RSS_MB; "rss-hard": process RSS or the complete
	// cgroup charge crossed celld's absolute cap). The wire value is the
	// shed reason, not a boolean.
	Shedding *string `json:"shedding"`
	// v0.4 reports both cgroup measurements in addition to process RSS and
	// allocator-adjusted RSS. They are null outside a readable Linux
	// cgroup, so pointers preserve "not measured" instead of exporting a
	// misleading zero.
	RSSBytes              int64  `json:"rss_bytes"`
	InUseBytes            int64  `json:"in_use_bytes"`
	CgroupWorkingSetBytes *int64 `json:"cgroup_working_set_bytes"`
	CgroupCurrentBytes    *int64 `json:"cgroup_current_bytes"`
}

// IsShedding reports whether the node is refusing new cells under pressure.
func (s *PodState) IsShedding() bool { return s.Shedding != nil }

// StateClient fetches /state from fleet pods.
type StateClient struct {
	HTTP *http.Client
}

func NewStateClient() *StateClient {
	return &StateClient{HTTP: &http.Client{Timeout: 3 * time.Second}}
}

func (c *StateClient) Fetch(ctx context.Context, podIP string) (*PodState, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	url := "http://" + net.JoinHostPort(podIP, strconv.Itoa(internalPort)) + "/state"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("/state returned %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 4<<20 {
		return nil, fmt.Errorf("/state exceeds 4 MiB")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("decoding /state: %w", err)
	}
	if _, failed := fields["error"]; failed {
		return nil, fmt.Errorf("/state reports a runtime error")
	}
	for _, key := range []string{"occupied", "restoring", "shedding"} {
		value, present := fields[key]
		if !present || (key != "shedding" && bytes.Equal(bytes.TrimSpace(value), []byte("null"))) {
			return nil, fmt.Errorf("/state missing %s", key)
		}
	}
	var state PodState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decoding /state: %w", err)
	}
	if state.Occupied < 0 || state.Restoring < 0 || state.Evicting < 0 {
		return nil, fmt.Errorf("/state has negative counters")
	}
	if d := state.Deployment; d != nil && (d.Version == "" || d.Generation < 1 || d.Swapping < 0) {
		return nil, fmt.Errorf("invalid deployment state")
	}
	return &state, nil
}

// FleetSweep polls every running pod of one fleet and returns the per-pod
// samples plus the fleet-wide restoring sum. An unreachable pod fails the
// sweep for rollout purposes (the gate must not step on missing data), so
// the error names the pod.
func (c *StateClient) FleetSweep(ctx context.Context, pods []corev1.Pod) (map[string]*PodState, int64, error) {
	states := make(map[string]*PodState, len(pods))
	var restoring int64
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(16)
	for i := range pods {
		pod := pods[i]
		group.Go(func() error {
			if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning || !pod.DeletionTimestamp.IsZero() {
				return fmt.Errorf("pod %s is unavailable", pod.Name)
			}
			state, err := c.Fetch(groupCtx, pod.Status.PodIP)
			if err != nil {
				return fmt.Errorf("pod %s: %w", pod.Name, err)
			}
			mu.Lock()
			states[pod.Name] = state
			restoring += state.Restoring
			mu.Unlock()
			return nil
		})
	}
	err := group.Wait()
	return states, restoring, err
}

// StatePoller is a manager Runnable that continuously exports fleet metrics.
// It runs on the leader only, so each series has one writer.
type StatePoller struct {
	Client   client.Client
	State    *StateClient
	Interval time.Duration
}

func (p *StatePoller) NeedLeaderElection() bool { return true }

func (p *StatePoller) Start(ctx context.Context) error {
	defer fleetMetrics.publish(nil)
	log := logf.FromContext(ctx).WithName("state-poller")
	interval := p.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.sweep(ctx); err != nil {
				log.Error(err, "Failed to sweep fleet state")
			}
		}
	}
}

func (p *StatePoller) sweep(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var apps platformv1alpha1.WorkerAppList
	if err := p.Client.List(ctx, &apps); err != nil {
		return err
	}
	var samples []prometheus.Metric
	add := func(name string, value float64, labels ...string) {
		keys := []string{labelNamespace, labelWorkerApp}
		if len(labels) == 3 {
			keys = append(keys, labelPod)
		}
		samples = append(samples, prometheus.MustNewConstMetric(prometheus.NewDesc(name, name, keys, nil), prometheus.GaugeValue, value, labels...))
	}
	for i := range apps.Items {
		app := &apps.Items[i]
		sampledAt := time.Now()
		var sts appsv1.StatefulSet
		var pods corev1.PodList
		err := p.Client.Get(ctx, client.ObjectKey{Namespace: app.Namespace, Name: fleetName(app)}, &sts)
		if err == nil {
			err = p.Client.List(ctx, &pods, client.InNamespace(app.Namespace), client.MatchingLabels(selectorLabels(app)))
		}
		complete := err == nil && ptr.Deref(sts.Spec.Replicas, 0) > 0 && int32(len(pods.Items)) == ptr.Deref(sts.Spec.Replicas, 0) && sts.Status.ObservedGeneration == sts.Generation
		for j := range pods.Items {
			if !metav1.IsControlledBy(&pods.Items[j], &sts) || !podReady(&pods.Items[j]) {
				complete = false
			}
		}
		states, _, sweepErr := p.State.FleetSweep(ctx, pods.Items)
		complete = complete && sweepErr == nil && len(states) == len(pods.Items)
		add("celld_fleet_complete", boolToGauge(complete), app.Namespace, app.Name)
		add("celld_fleet_sample_timestamp_seconds", float64(sampledAt.Unix()), app.Namespace, app.Name)
		for j := range pods.Items {
			pod := &pods.Items[j]
			labels := []string{app.Namespace, app.Name, pod.Name}
			restarts, fenced := celldRestarts(pod)
			add("celld_container_restarts", float64(restarts), labels...)
			add("celld_self_fenced", boolToGauge(fenced), labels...)
			state, up := states[pod.Name]
			add("celld_state_up", boolToGauge(up), labels...)
			if !up {
				continue
			}
			values := map[string]float64{
				"celld_resident_cells": float64(state.Occupied), "celld_evicting": float64(state.Evicting),
				"celld_restoring": float64(state.Restoring), "celld_shedding": boolToGauge(state.IsShedding()),
				"celld_resident_cell_utilization": float64(state.Occupied) / float64(maxResidentCells(app)),
				"celld_rss_bytes":                 float64(state.RSSBytes), "celld_in_use_bytes": float64(state.InUseBytes),
			}
			if state.CgroupCurrentBytes != nil {
				values["celld_cgroup_current_bytes"] = float64(*state.CgroupCurrentBytes)
			}
			if state.CgroupWorkingSetBytes != nil {
				values["celld_cgroup_working_set_bytes"] = float64(*state.CgroupWorkingSetBytes)
			}
			for name, value := range values {
				add(name, value, labels...)
			}
		}
	}
	fleetMetrics.publish(samples)
	return nil
}

// celldRestarts reads the celld container's restart count and whether its
// last termination was a self-fence.
func celldRestarts(pod *corev1.Pod) (restarts int32, selfFenced bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != celldContainerName {
			continue
		}
		restarts = cs.RestartCount
		if t := cs.LastTerminationState.Terminated; t != nil {
			selfFenced = t.ExitCode == selfFenceExitCode
		}
		return restarts, selfFenced
	}
	return 0, false
}

func boolToGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
