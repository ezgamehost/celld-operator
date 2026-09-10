package chart

import (
	"os/exec"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

const httpMode = "http"
const certificateMode = "cert-manager"
const disabledMode = "disabled"

func TestMetricsAndNamespaceRendering(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is required for chart rendering tests")
	}
	for _, mode := range []string{httpMode, "self-signed", certificateMode, disabledMode} {
		t.Run(mode, func(t *testing.T) {
			args := []string{"template", "test", "../../dist/chart", "--namespace", "operator",
				"--set", "prometheus.enable=true", "--set", "operator.watchNamespace=tenant"}
			switch mode {
			case httpMode:
				args = append(args, "--set", "metrics.insecure=true", "--set", "metrics.port=8080")
			case certificateMode:
				args = append(args, "--set", "certmanager.enable=true")
			case disabledMode:
				args = append(args, "--set", "metrics.enable=false")
			}
			output, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("helm: %v %s", err, output)
			}
			foundManager, foundMonitor, foundRole := false, false, false
			for doc := range strings.SplitSeq(string(output), "\n---") {
				var object unstructured.Unstructured
				if err := yaml.Unmarshal([]byte(doc), &object.Object); err != nil {
					t.Fatal(err)
				}
				switch object.GetKind() {
				case "Deployment":
					foundManager = true
					assertManager(t, object, mode)
				case "ServiceMonitor":
					foundMonitor = true
					endpoints, _, _ := unstructured.NestedSlice(object.Object, "spec", "endpoints")
					endpoint := endpoints[0].(map[string]any)
					if endpoint["honorLabels"] != true {
						t.Fatal("fleet namespace/pod labels would be rewritten")
					}
					if mode == httpMode && (endpoint["scheme"] != httpMode ||
						endpoint["port"] != httpMode || endpoint["tlsConfig"] != nil) {
						t.Fatal("HTTP scrape misconfigured")
					}
				case "Role":
					if object.GetName() == "celld-operator-manager-role" {
						foundRole = true
						if object.GetNamespace() != "tenant" {
							t.Fatal("manager Role has wrong scope")
						}
					}
				case "ClusterRole":
					if object.GetName() == "celld-operator-manager-role" {
						t.Fatal("namespace-scoped operator has cluster-wide workload RBAC")
					}
				}
			}
			if !foundManager || !foundRole || (foundMonitor == (mode == disabledMode)) {
				t.Fatalf("unexpected resources: manager=%v role=%v monitor=%v", foundManager, foundRole, foundMonitor)
			}
		})
	}
}

func assertManager(t *testing.T, object unstructured.Unstructured, mode string) {
	t.Helper()
	containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	manager := containers[0].(map[string]any)
	flags, _, _ := unstructured.NestedStringSlice(manager, "args")
	joined := strings.Join(flags, " ")
	if strings.Count(joined, "--metrics-bind-address=") != 1 || !strings.Contains(joined, "--watch-namespace=tenant") {
		t.Fatalf("bad flags: %s", joined)
	}
	if mode == httpMode && !strings.Contains(joined, "--metrics-secure=false") {
		t.Fatal("HTTP still uses TLS")
	}
	if mode == certificateMode && !strings.Contains(joined, "--metrics-cert-path=") {
		t.Fatal("certificate not passed to manager")
	}
	if mode == disabledMode && !strings.Contains(joined, "--metrics-bind-address=0") {
		t.Fatal("metrics still enabled")
	}
}
