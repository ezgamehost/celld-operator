//go:build e2e

package e2e

import (
	"os/exec"
	"strings"
	"time"

	"github.com/ezgamehost/celld-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const fixtureImage = "example.com/celld-fixture:v0.4.0"
const fixtureNextImage = "example.com/celld-fixture:v0.4.1"

func verifyWorkerAppLifecycle() {
	By("allowing the deterministic test runtime on this isolated operator")
	patch := `[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--allowed-image-prefixes=ghcr.io/denoland/celld:,example.com/celld-fixture:"}]`
	_, err := utils.Run(exec.Command("kubectl", "patch", "deployment", "celld-operator-controller-manager", "-n", namespace, "--type=json", "-p", patch))
	Expect(err).NotTo(HaveOccurred())
	_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/celld-operator-controller-manager", "-n", namespace, "--timeout=120s"))
	Expect(err).NotTo(HaveOccurred())
	manifest := `apiVersion: celld-operator.io/v1alpha1
kind: WorkerApp
metadata:
  name: lifecycle
  namespace: celld-operator-system
spec:
  appVersion: deploy-a
  celld:
    image: example.com/celld-fixture:v0.4.0
  bucket:
    name: s3://unused-by-test-fixture
  replicas: 1
  resources:
    memoryGi: 1
    cpuMillis: 10
  storage:
    sizeGi: 1
`
	apply := exec.Command("kubectl", "apply", "-f", "-")
	apply.Stdin = strings.NewReader(manifest)
	_, err = utils.Run(apply)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workerapp", "lifecycle", "-n", namespace, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pvc", "watch-lifecycle-celld-0", "-n", namespace, "--ignore-not-found"))
	})
	ready := func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get", "workerapp", "lifecycle", "-n", namespace, "-o", "jsonpath={.status.phase}:{.status.rolledOutAppVersion}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("Ready:deploy-a"))
	}
	Eventually(ready, 180*time.Second, time.Second).Should(Succeed())
	logs, err := utils.Run(exec.Command("kubectl", "logs", "lifecycle-celld-0", "-n", namespace))
	Expect(err).NotTo(HaveOccurred())
	Expect(logs).To(ContainSubstring("Boot 1"))
	By("rolling a real StatefulSet and retaining its volume across replacement")
	_, err = utils.Run(exec.Command("kubectl", "patch", "workerapp", "lifecycle", "-n", namespace, "--type=merge", "-p", `{"spec":{"celld":{"image":"example.com/celld-fixture:v0.4.1"}}}`))
	Expect(err).NotTo(HaveOccurred())
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get", "pod", "lifecycle-celld-0", "-n", namespace, "-o", "jsonpath={.spec.containers[0].image}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal(fixtureNextImage))
	}, 180*time.Second, time.Second).Should(Succeed())
	Eventually(ready, 180*time.Second, time.Second).Should(Succeed())
	logs, err = utils.Run(exec.Command("kubectl", "logs", "lifecycle-celld-0", "-n", namespace))
	Expect(err).NotTo(HaveOccurred())
	Expect(logs).To(ContainSubstring("Boot 2"))
	By("rejecting an unsafe fleet durability configuration at admission")
	_, err = utils.Run(exec.Command("kubectl", "patch", "workerapp", "lifecycle", "-n", namespace, "--type=merge", "-p", `{"spec":{"durability":"fleet"}}`))
	Expect(err).To(HaveOccurred())
}
