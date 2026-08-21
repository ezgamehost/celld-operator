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
	"strings"
	"testing"
)

const (
	// testCelldImageV021 is the last v0.2 release: paired with
	// testCelldImage it exercises both the rolling-safe upgrade and the
	// lossy downgrade (docs/celld-behaviors.md F8).
	testCelldImageV021 = "ghcr.io/denoland/celld:v0.2.1"
	// reasonMixedFleet is the substring of the v0.1/v0.2 boundary's reason
	// that the tests key on.
	reasonMixedFleet = "cannot share a fleet"
)

// The table mirrors celld's release notes (docs/celld-behaviors.md F8):
// v0.1 <-> v0.2 forbids a mixed fleet in both directions; v0.2 -> v0.3 is
// rolling-safe but v0.3 -> v0.2 can lose acknowledged writes.
func TestIsBreakingUpgrade(t *testing.T) {
	const (
		v01 = "ghcr.io/denoland/celld:v0.1.0"
		v02 = "ghcr.io/denoland/celld:v0.2.0"
		v03 = "ghcr.io/denoland/celld:v0.3.0"
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
		{"same image", v03, v03, false, ""},
		{"unparseable tags are not refused", "ghcr.io/denoland/celld:latest", v03, false, ""},
		{"a tag without a minor is not refused", "ghcr.io/denoland/celld:v1", v03, false, ""},
		{"registry with a port still parses the tag", "registry:5000/celld:v0.1.0", "registry:5000/celld:v0.2.0", true, reasonMixedFleet},
		{"a future minor with no flagged boundary", v03, "ghcr.io/denoland/celld:v0.4.0", false, ""},
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

	t.Run("a two-boundary downgrade names both hazards", func(t *testing.T) {
		reason := breakingReason(v03, v01)
		if !strings.Contains(reason, reasonMixedFleet) || !strings.Contains(reason, "sealed epoch") {
			t.Errorf("breakingReason(v0.3, v0.1) = %q, want both boundary reasons", reason)
		}
	})
}
