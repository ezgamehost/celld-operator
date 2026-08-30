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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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

var (
	stateLabels = []string{labelNamespace, labelWorkerApp, labelPod}

	metricResidentCells = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_resident_cells",
		Help: "Resident (occupied) cells on a fleet pod, from celld /state.",
	}, stateLabels)
	metricEvicting = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_evicting",
		Help: "Cells the pod is currently evicting, from celld /state.",
	}, stateLabels)
	metricRestoring = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_restoring",
		Help: "Cold routes holding or awaiting an activation permit, from celld /state.",
	}, stateLabels)
	metricUtilization = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_resident_cell_utilization",
		Help: "occupied / CELLD_MAX_RESIDENT_CELLS per pod (0..1); the primary autoscaling signal.",
	}, stateLabels)
	metricShedding = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_shedding",
		Help: "1 while the pod reports pressure shedding; the hard out-of-capacity signal.",
	}, stateLabels)
	metricUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_state_up",
		Help: "1 if the pod's internal /state endpoint answered the last poll.",
	}, stateLabels)
	metricRSSBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_rss_bytes",
		Help: "Resident set size of the celld process, from /state.",
	}, stateLabels)
	metricInUseBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_in_use_bytes",
		Help: "Process RSS minus allocator retention, from /state.",
	}, stateLabels)
	metricCgroupWorkingSetBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_cgroup_working_set_bytes",
		Help: "Active cgroup memory used by celld's v0.4 pressure threshold, from /state.",
	}, stateLabels)
	metricCgroupCurrentBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_cgroup_current_bytes",
		Help: "Complete cgroup charge used by celld's v0.4 absolute memory cap, from /state.",
	}, stateLabels)
	metricRestarts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_container_restarts",
		Help: "Kubelet restart count of the celld container; celld relies on the supervisor restarting it after a self-fence.",
	}, stateLabels)
	metricSelfFenced = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "celld_self_fenced",
		Help: "1 if the celld container's last termination was a self-fence (exit code 3): it lost its lease or its storage probe failed.",
	}, stateLabels)
)

// selfFenceExitCode is what celld exits with after logging "SELF-FENCE:"
// (celld docs/fencing.md); the kubelet restarts it, and this is the one
// signal that distinguishes a fence from any other crash.
const selfFenceExitCode = 3

func init() {
	metrics.Registry.MustRegister(
		metricResidentCells, metricEvicting, metricRestoring,
		metricUtilization, metricShedding, metricUp,
		metricRSSBytes, metricInUseBytes,
		metricCgroupWorkingSetBytes, metricCgroupCurrentBytes,
		metricRestarts, metricSelfFenced,
	)
}

// PodState is one pod's /state sample. The response schema is celld's alpha
// operator API (actor.rs state_json): keep parsing tolerant, fail per-pod
// not per-fleet, and pin operator and celld releases together
// (docs/celld-behaviors.md). Fields added across releases decode as zero
// or nil so an older node does not break the whole fleet sweep.
type PodState struct {
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
	url := fmt.Sprintf("http://%s:%d/state", podIP, internalPort)
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
	var state PodState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return nil, fmt.Errorf("decoding /state: %w", err)
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
	for i := range pods {
		pod := &pods[i]
		if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		state, err := c.Fetch(ctx, pod.Status.PodIP)
		if err != nil {
			return states, restoring, fmt.Errorf("pod %s: %w", pod.Name, err)
		}
		states[pod.Name] = state
		restoring += state.Restoring
	}
	return states, restoring, nil
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
	log := logf.FromContext(ctx).WithName("state-poller")
	interval := p.Interval
	if interval == 0 {
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
				log.Error(err, "fleet state sweep failed")
			}
		}
	}
}

func (p *StatePoller) sweep(ctx context.Context) error {
	var apps platformv1alpha1.WorkerAppList
	if err := p.Client.List(ctx, &apps); err != nil {
		return err
	}
	// Reset then repopulate: pods and fleets come and go, and stale series
	// would keep autoscaling on dead data. Single writer (leader-only).
	metricResidentCells.Reset()
	metricEvicting.Reset()
	metricRestoring.Reset()
	metricUtilization.Reset()
	metricShedding.Reset()
	metricUp.Reset()
	metricRSSBytes.Reset()
	metricInUseBytes.Reset()
	metricCgroupWorkingSetBytes.Reset()
	metricCgroupCurrentBytes.Reset()
	metricRestarts.Reset()
	metricSelfFenced.Reset()

	for i := range apps.Items {
		app := &apps.Items[i]
		var pods corev1.PodList
		if err := p.Client.List(ctx, &pods,
			client.InNamespace(app.Namespace),
			client.MatchingLabels(selectorLabels(app))); err != nil {
			return err
		}
		maxCells := float64(maxResidentCells(app))
		for j := range pods.Items {
			pod := &pods.Items[j]
			if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
				continue
			}
			labels := prometheus.Labels{
				labelNamespace: app.Namespace, labelWorkerApp: app.Name, labelPod: pod.Name,
			}
			// Restart bookkeeping comes from the pod, not /state, so it is
			// reported even while the node is down (F12).
			restarts, selfFenced := celldRestarts(pod)
			metricRestarts.With(labels).Set(float64(restarts))
			metricSelfFenced.With(labels).Set(boolToGauge(selfFenced))
			state, err := p.State.Fetch(ctx, pod.Status.PodIP)
			if err != nil {
				metricUp.With(labels).Set(0)
				continue
			}
			metricUp.With(labels).Set(1)
			metricResidentCells.With(labels).Set(float64(state.Occupied))
			metricEvicting.With(labels).Set(float64(state.Evicting))
			metricRestoring.With(labels).Set(float64(state.Restoring))
			metricUtilization.With(labels).Set(float64(state.Occupied) / maxCells)
			metricShedding.With(labels).Set(boolToGauge(state.IsShedding()))
			metricRSSBytes.With(labels).Set(float64(state.RSSBytes))
			metricInUseBytes.With(labels).Set(float64(state.InUseBytes))
			if state.CgroupWorkingSetBytes != nil {
				metricCgroupWorkingSetBytes.With(labels).Set(float64(*state.CgroupWorkingSetBytes))
			}
			if state.CgroupCurrentBytes != nil {
				metricCgroupCurrentBytes.With(labels).Set(float64(*state.CgroupCurrentBytes))
			}
		}
	}
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
