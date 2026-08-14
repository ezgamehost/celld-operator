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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
)

const (
	testCelldImage    = "ghcr.io/denoland/celld:v0.2.0"
	testCelldImageOld = "ghcr.io/denoland/celld:v0.1.0"
)

var _ = Describe("WorkerApp Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		fleetKey := types.NamespacedName{
			Name:      resourceName + "-celld",
			Namespace: resourceNamespace,
		}
		workerapp := &platformv1alpha1.WorkerApp{}

		reconciler := func() *WorkerAppReconciler {
			return &WorkerAppReconciler{
				Client:            k8sClient,
				Scheme:            k8sClient.Scheme(),
				State:             NewStateClient(),
				Reader:            k8sClient,
				Deploys:           NewDeployTracker(0),
				GatewayName:       "edge",
				GatewayNamespace:  "infra",
				PrometheusURL:     "http://prometheus.test:9090",
				OperatorNamespace: "celld-operator-system",
				OperatorPrincipal: "cluster.local/ns/celld-operator-system/sa/celld-operator",
			}
		}

		BeforeEach(func() {
			By("creating the custom resource for the Kind WorkerApp")
			err := k8sClient.Get(ctx, typeNamespacedName, workerapp)
			if err != nil && errors.IsNotFound(err) {
				resource := &platformv1alpha1.WorkerApp{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					// The minimum valid spec: appVersion, celld.image, and
					// bucket.name are required (see workerapp_types.go).
					Spec: platformv1alpha1.WorkerAppSpec{
						AppVersion: "sha-test",
						Celld: platformv1alpha1.CelldSpec{
							Image: testCelldImage,
						},
						Bucket: platformv1alpha1.BucketSpec{
							Name: "s3://test-cells/apps/test",
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &platformv1alpha1.WorkerApp{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance WorkerApp")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			// envtest has no garbage collector; remove the owned fleet so
			// the next spec starts clean.
			sts := &appsv1.StatefulSet{}
			if err := k8sClient.Get(ctx, fleetKey, sts); err == nil {
				Expect(k8sClient.Delete(ctx, sts)).To(Succeed())
			}
		})

		It("should reconcile the full fleet", func() {
			By("Reconciling twice: create pass, then steady pass")
			r := reconciler()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("creating the StatefulSet with the celld guardrails")
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, fleetKey, sts)).To(Succeed())
			container := sts.Spec.Template.Spec.Containers[0]
			Expect(container.Image).To(Equal(testCelldImage))
			// Liveness must never be an HTTP probe on the health path: it
			// answers 503 during a graceful drain (DESIGN.md §6).
			Expect(container.LivenessProbe.HTTPGet).To(BeNil())
			Expect(container.LivenessProbe.TCPSocket).NotTo(BeNil())
			Expect(container.ReadinessProbe.HTTPGet.Path).To(Equal("/__celld/health"))
			Expect(*sts.Spec.Template.Spec.TerminationGracePeriodSeconds).
				To(BeNumerically(">", int64(shutdownDrainMs/1000)))
			Expect(sts.Spec.Template.Annotations).To(HaveKeyWithValue(appVersionAnnotation, "sha-test"))
			env := map[string]string{}
			for _, e := range container.Env {
				env[e.Name] = e.Value
			}
			Expect(env).To(HaveKeyWithValue("CELLD_BUCKET", "s3://test-cells/apps/test"))
			// Explicit RSS bound ≈80% of the 8Gi default limit.
			Expect(env).To(HaveKeyWithValue("CELLD_MAX_RSS_MB", "6553"))
			Expect(env["CELLD_ADVERTISE"]).To(ContainSubstring("test-resource-celld-internal"))

			By("creating both Services, the NetworkPolicy, and the PDB")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, fleetKey, svc)).To(Succeed())
			headless := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-celld-internal", Namespace: resourceNamespace,
			}, headless)).To(Succeed())
			Expect(headless.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			// Peers must reach draining pods to take over cells.
			Expect(headless.Spec.PublishNotReadyAddresses).To(BeTrue())
			netpol := &networkingv1.NetworkPolicy{}
			Expect(k8sClient.Get(ctx, fleetKey, netpol)).To(Succeed())
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, fleetKey, pdb)).To(Succeed())

			By("reporting status: converging, credentials configured, Istio absent tolerated")
			updated := &platformv1alpha1.WorkerApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(platformv1alpha1.PhasePending))
			Expect(meta.IsStatusConditionTrue(updated.Status.Conditions, condBucketCredentialsReady)).To(BeTrue())
			mesh := meta.FindStatusCondition(updated.Status.Conditions, condMeshPolicyReady)
			Expect(mesh).NotTo(BeNil())
			Expect(mesh.Reason).To(Equal("IstioUnavailable"))
		})

		It("should emit a v1 Ingress with TLS and route policy in ingress mode", func() {
			By("adding hostnames and reconciling in ingress mode")
			resource := &platformv1alpha1.WorkerApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			resource.Spec.Hostnames = []string{"test.example.com"}
			resource.Spec.WebSockets = true
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())

			r := reconciler()
			r.IngressMode = IngressModeIngress
			r.IngressClassName = "nginx"
			r.ClusterIssuer = "letsencrypt"
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, fleetKey, ingress)).To(Succeed())
			Expect(*ingress.Spec.IngressClassName).To(Equal("nginx"))
			Expect(ingress.Spec.Rules[0].Host).To(Equal("test.example.com"))
			backend := ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service
			Expect(backend.Name).To(Equal(resourceName + "-celld"))
			Expect(backend.Port.Number).To(Equal(int32(8080)))
			// cert-manager TLS via the cluster issuer.
			Expect(ingress.Annotations).To(HaveKeyWithValue("cert-manager.io/cluster-issuer", "letsencrypt"))
			Expect(ingress.Spec.TLS[0].SecretName).To(Equal(resourceName + "-celld-tls"))
			// Route policy as annotations: drain-503 retries always, long
			// proxy timeouts for the WebSocket profile.
			Expect(ingress.Annotations["nginx.ingress.kubernetes.io/proxy-next-upstream"]).
				To(ContainSubstring("http_503"))
			Expect(ingress.Annotations).To(HaveKey("nginx.ingress.kubernetes.io/proxy-read-timeout"))

			By("cleaning up the Ingress (no GC in envtest)")
			Expect(k8sClient.Delete(ctx, ingress)).To(Succeed())
		})

		It("should shape the serving Service from spec.service", func() {
			By("requesting an annotated LoadBalancer Service")
			resource := &platformv1alpha1.WorkerApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			resource.Spec.Service = platformv1alpha1.ServiceSpec{
				Type: corev1.ServiceTypeLoadBalancer,
				Annotations: map[string]string{
					"service.beta.kubernetes.io/do-loadbalancer-hostname": "internal.example.com",
				},
			}
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())

			r := reconciler()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, fleetKey, svc)).To(Succeed())
			Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeLoadBalancer))
			Expect(svc.Annotations).To(HaveKeyWithValue(
				"service.beta.kubernetes.io/do-loadbalancer-hostname", "internal.example.com"))
			// The internal peer Service is untouched by spec.service.
			headless := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-celld-internal", Namespace: resourceNamespace,
			}, headless)).To(Succeed())
			Expect(headless.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		})

		It("should hold the live version when auto tracking cannot reach the bucket", func() {
			By("creating the fleet pinned, then switching to auto")
			r := reconciler()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			resource := &platformv1alpha1.WorkerApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			resource.Spec.AppVersion = AppVersionAuto
			// Point at an unreachable endpoint so the pointer read fails.
			resource.Spec.Bucket.Endpoint = "http://127.0.0.1:1"
			resource.Spec.Bucket.CredentialsFrom.SecretRef = ""
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("reporting the failure and keeping the fleet on its live version")
			updated := &platformv1alpha1.WorkerApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			cond := meta.FindStatusCondition(updated.Status.Conditions, condDeployTrackingReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal("BucketUnreachable"))
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, fleetKey, sts)).To(Succeed())
			// An unreachable bucket must not trigger a rollout to nowhere:
			// the template keeps serving the previously pinned version.
			Expect(sts.Spec.Template.Annotations).To(HaveKeyWithValue(appVersionAnnotation, "sha-test"))
		})

		It("should refuse a rolling update across a breaking celld boundary", func() {
			r := reconciler()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("bumping the celld image across the v0.1 -> v0.2 boundary shape")
			resource := &platformv1alpha1.WorkerApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			// Seed the live StatefulSet with a v0.1 image so the transition
			// to the spec's v0.2 image crosses the flagged boundary.
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, fleetKey, sts)).To(Succeed())
			sts.Spec.Template.Spec.Containers[0].Image = testCelldImageOld
			// Stale the hash too: rollout detection compares the template
			// hash annotation, and the seeded template must read as old.
			sts.Annotations[templateHashAnnotation] = "stale-v0.1-template"
			Expect(k8sClient.Update(ctx, sts)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			updated := &platformv1alpha1.WorkerApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(platformv1alpha1.PhaseDegraded))
			Expect(updated.Status.Rollout.WaitingOn).To(ContainSubstring("not rolling-safe"))

			By("confirming the fleet was not touched")
			Expect(k8sClient.Get(ctx, fleetKey, sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal(testCelldImageOld))
		})
	})
})
