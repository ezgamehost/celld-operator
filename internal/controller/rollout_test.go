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
	"encoding/json"
	"strings"
	"testing"
)

const (
	// testCelldImageV021 is the last v0.2 release: paired with v0.3 it
	// exercises both the rolling-safe upgrade and the lossy downgrade
	// (docs/celld-behaviors.md F8).
	testCelldImageV021 = "ghcr.io/denoland/celld:v0.2.1"
	// reasonMixedFleet is shared by the stop-all boundaries' reasons and
	// keeps assertions focused on the operational requirement.
	reasonMixedFleet = "cannot share a fleet"
)

// The table mirrors celld's release notes (docs/celld-behaviors.md F8):
// v0.1 <-> v0.2 and v0.3 <-> v0.4 forbid mixed fleets; v0.2 -> v0.3 is
// rolling-safe but v0.3 -> v0.2 can lose acknowledged writes.
func TestIsBreakingUpgrade(t *testing.T) {
	const (
		v01 = "ghcr.io/denoland/celld:v0.1.0"
		v02 = "ghcr.io/denoland/celld:v0.2.0"
		v03 = "ghcr.io/denoland/celld:v0.3.0"
		v04 = "ghcr.io/denoland/celld:v0.4.0"
	)
	cases := []struct {
		name     string
		from, to string
		want     bool
		reason   string // substring expected in the reason when want is true
	}{
		{"the documented v0.1 to v0.2 boundary", v01, v02, true, reasonMixedFleet},
		{"the v0.1/v0.2 boundary crossed backwards", v02, "ghcr.io/denoland/celld:v0.1.3", true, reasonMixedFleet},
		{"a patch bump inside one minor", v02, testCelldImageV021, false, ""},
		{"v0.2 to v0.3 is rolling-safe upstream", testCelldImageV021, v03, false, ""},
		{"v0.3 to v0.2 is a lossy downgrade", v03, testCelldImageV021, true, "sealed epoch"},
		{"a jump that skips over a flagged boundary still crosses it", v01, v03, true, reasonMixedFleet},
		{"a downgrade across two boundaries reports both", v03, v01, true, "sealed epoch"},
		{"v0.3 to v0.4 cannot mix peer tunnel protocols", v03, v04, true, "peer tunnel"},
		{"v0.4 to v0.3 cannot expose unreadable KV references", v04, v03, true, "Workers KV"},
		{"same image", v04, v04, false, ""},
		{"unparseable tags are not refused", "ghcr.io/denoland/celld:latest", v04, false, ""},
		{"a tag without a minor is not refused", "ghcr.io/denoland/celld:v1", v04, false, ""},
		{"registry with a port still parses the tag", "registry:5000/celld:v0.1.0", "registry:5000/celld:v0.2.0", true, reasonMixedFleet},
		{"a tag with a digest suffix parses before the digest", "registry:5000/celld:v0.1.0@sha256:7c222fb2927d828af22f592134e8932480637c0d7f076771e81cf2acd7204ecf", "registry:5000/celld:v0.2.0", true, reasonMixedFleet},
		{"a future minor with no flagged boundary", v04, "ghcr.io/denoland/celld:v0.5.0", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason := breakingReason(tc.from, tc.to)
			if got := isBreakingUpgrade(tc.from, tc.to); got != tc.want {
				t.Fatalf("isBreakingUpgrade(%q, %q) = %v, want %v (reason %q)", tc.from, tc.to, got, tc.want, reason)
			}
			if tc.want && !strings.Contains(reason, tc.reason) {
				t.Errorf("breakingReason(%q, %q) = %q, want it to mention %q", tc.from, tc.to, reason, tc.reason)
			}
		})
	}

	t.Run("a three-boundary downgrade names every hazard", func(t *testing.T) {
		reason := breakingReason(v04, v01)
		if !strings.Contains(reason, reasonMixedFleet) ||
			!strings.Contains(reason, "sealed epoch") ||
			!strings.Contains(reason, "peer tunnel") {
			t.Errorf("breakingReason(v0.4, v0.1) = %q, want all boundary reasons", reason)
		}
	})
}

func TestPodStateV04MemoryFields(t *testing.T) {
	const payload = `{
		"occupied": 7,
		"shedding": "memory",
		"rss_bytes": 100,
		"in_use_bytes": 80,
		"cgroup_working_set_bytes": 90,
		"cgroup_current_bytes": 120
	}`
	var state PodState
	if err := json.Unmarshal([]byte(payload), &state); err != nil {
		t.Fatalf("decode v0.4 /state: %v", err)
	}
	if state.CgroupWorkingSetBytes == nil || *state.CgroupWorkingSetBytes != 90 {
		t.Errorf("cgroup_working_set_bytes = %v, want 90", state.CgroupWorkingSetBytes)
	}
	if state.CgroupCurrentBytes == nil || *state.CgroupCurrentBytes != 120 {
		t.Errorf("cgroup_current_bytes = %v, want 120", state.CgroupCurrentBytes)
	}
	if !state.IsShedding() {
		t.Error("v0.4 shedding reason should mark the pod as shedding")
	}

	var older PodState
	if err := json.Unmarshal([]byte(`{"rss_bytes":100,"in_use_bytes":80}`), &older); err != nil {
		t.Fatalf("decode pre-v0.4 /state: %v", err)
	}
	if older.CgroupWorkingSetBytes != nil || older.CgroupCurrentBytes != nil {
		t.Errorf("absent cgroup fields decoded as %v, %v; want nil", older.CgroupWorkingSetBytes, older.CgroupCurrentBytes)
	}
}
