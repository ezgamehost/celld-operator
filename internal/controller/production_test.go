package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const testWorkerAppKind = "WorkerApp"
const testAppHostname = "app.example.com"

const healthyState = `{"occupied":4,"restoring":0,"shedding":null,"deployment":{"version":"deploy-a","generation":1,"draining":[],"swapping":0}}`

func productionFixture(t *testing.T) (*WorkerAppReconciler, *platformv1alpha1.WorkerApp, *appsv1.StatefulSet) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, autoscalingv2.AddToScheme, platformv1alpha1.AddToScheme, gatewayv1.Install, networkingv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	app := &platformv1alpha1.WorkerApp{ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", UID: "app"}, Spec: platformv1alpha1.WorkerAppSpec{AppVersion: "deploy-a", Celld: platformv1alpha1.CelldSpec{Image: testCelldImage}, Bucket: platformv1alpha1.BucketSpec{Name: "s3://bucket/app"}}}
	sts := buildStatefulSet(app, "", app.Spec.AppVersion)
	sts.UID = "sts"
	sts.Generation = 1
	sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.SchemeGroupVersion.String(), Kind: testWorkerAppKind, Name: app.Name, UID: app.UID, Controller: ptr.To(true)}}
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: 3, UpdatedReplicas: 3, Replicas: 3, UpdateRevision: "new"}
	objects := make([]client.Object, 0, 5)
	objects = append(objects, app, sts)
	for i := range 3 {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", sts.Name, i), Namespace: app.Namespace, Labels: selectorLabels(app), OwnerReferences: []metav1.OwnerReference{{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: testStatefulSetKind, Name: sts.Name, UID: sts.UID, Controller: ptr.To(true)}}}, Status: corev1.PodStatus{PodIP: fmt.Sprintf("10.0.0.%d", i+1), Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
		pod.Labels["controller-revision-hash"] = "new"
		objects = append(objects, pod)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(app, sts, &corev1.Pod{}).Build()
	r := &WorkerAppReconciler{Client: c, Reader: c, Scheme: scheme, State: responseClient(healthyState), Deploys: NewDeployTracker(time.Minute), IngressMode: IngressModeNone}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(sts), sts); err != nil {
		t.Fatal(err)
	}
	return r, app, sts
}
func responseClient(body string) *StateClient {
	return &StateClient{HTTP: &http.Client{Transport: stateRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
}
func TestStateValidationFailsClosed(t *testing.T) {
	for _, body := range []string{`{}`, `{"error":"actor_stopped"}`, `{"occupied":1,"restoring":null,"shedding":null}`, `{"occupied":1,"restoring":-1,"shedding":null}`, healthyState + `{}`, strings.Repeat(" ", 4<<20) + healthyState} {
		if _, err := responseClient(body).Fetch(context.Background(), "10.0.0.1"); err == nil {
			t.Fatalf("accepted invalid state %.80s", body)
		}
	}
}
func TestRollingGateRequiresCompleteFleetAndSettle(t *testing.T) {
	r, app, sts := productionFixture(t)
	setStsPartition(sts, 3)
	sts.Annotations[settleAnnotation] = time.Now().Add(time.Minute).Format(time.RFC3339Nano)
	if err := r.Update(context.Background(), sts); err != nil {
		t.Fatal(err)
	}
	out, err := r.rolloutStep(context.Background(), app, sts)
	if err != nil || out.Partition != 3 {
		t.Fatalf("settle gate moved: %+v %v", out, err)
	}
	sts.Annotations[settleAnnotation] = time.Now().Add(-time.Minute).Format(time.RFC3339Nano)
	if err := r.Update(context.Background(), sts); err != nil {
		t.Fatal(err)
	}
	r.State = responseClient(`{"error":"actor_stopped"}`)
	out, err = r.rolloutStep(context.Background(), app, sts)
	if err != nil || out.Partition != 3 {
		t.Fatalf("invalid state released ordinal: %+v %v", out, err)
	}
	r.State = responseClient(healthyState)
	out, err = r.rolloutStep(context.Background(), app, sts)
	if err != nil || out.Partition != 2 {
		t.Fatalf("healthy fleet did not progress: %+v %v", out, err)
	}
	var pod corev1.Pod
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: app.Namespace, Name: sts.Name + "-0"}, &pod); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.observeFleet(context.Background(), app, sts); err == nil {
		t.Fatal("accepted a missing ordinal")
	}
}
func TestKEDAPauseWaitsForAcknowledgementAndHPA(t *testing.T) {
	r, app, sts := productionFixture(t)
	app.Spec.Autoscaling = &platformv1alpha1.AutoscalingSpec{Enabled: true, MinReplicas: 2, MaxReplicas: 10}
	paused, err := r.pauseScaling(context.Background(), app)
	if err != nil || paused {
		t.Fatalf("pause request: %v %v", paused, err)
	}
	paused, err = r.pauseScaling(context.Background(), app)
	if err != nil || paused {
		t.Fatalf("pause not acknowledged: %v %v", paused, err)
	}
	obj := buildScaledObject(app, "", true)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal([]metav1.Condition{{Type: "Paused", Status: metav1.ConditionTrue}})
	var conditions []any
	if err := json.Unmarshal(raw, &conditions); err != nil {
		t.Fatal(err)
	}
	obj.Object["status"] = map[string]any{"conditions": conditions}
	if err := r.Update(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "hpa", Namespace: app.Namespace}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: testStatefulSetKind, Name: sts.Name}}}
	if err := r.Create(context.Background(), hpa); err != nil {
		t.Fatal(err)
	}
	paused, err = r.pauseScaling(context.Background(), app)
	if err != nil || paused {
		t.Fatalf("live HPA ignored: %v %v", paused, err)
	}
	if err := r.Delete(context.Background(), hpa); err != nil {
		t.Fatal(err)
	}
	paused, err = r.pauseScaling(context.Background(), app)
	if err != nil || !paused {
		t.Fatalf("acknowledged pause blocked: %v %v", paused, err)
	}
}
func TestRecreateDoesNotTrustStaleReplicaStatus(t *testing.T) {
	r, app, sts := productionFixture(t)
	sts.Spec.Replicas = ptr.To(int32(0))
	sts.Status.Replicas = 0
	if err := r.Update(context.Background(), sts); err != nil {
		t.Fatal(err)
	}
	desired := buildStatefulSet(app, "changed", "deploy-b")
	out, err := r.recreateStep(context.Background(), app, sts, desired, "")
	if err != nil || *sts.Spec.Replicas != 0 || out.WaitingOn == "starting new fleet" {
		t.Fatalf("started with old pods present: %+v %v", out, err)
	}
}
func TestServingVersionMustBeObserved(t *testing.T) {
	r, app, sts := productionFixture(t)
	out, err := r.steadyFleet(context.Background(), app, sts, "deploy-b")
	if err != nil || out.RolledOut {
		t.Fatalf("claimed wrong version: %+v %v", out, err)
	}
	out, err = r.steadyFleet(context.Background(), app, sts, "deploy-a")
	if err != nil || !out.RolledOut {
		t.Fatalf("did not observe convergence: %+v %v", out, err)
	}
	app.Spec.DeploymentPolicy = "InPlace"
	if templateHash(buildPodTemplate(app, "", "a")) != templateHash(buildPodTemplate(app, "", "b")) {
		t.Fatal("in-place publication restarted pods")
	}
}
func TestStorageAndSecurityProfile(t *testing.T) {
	_, app, _ := productionFixture(t)
	app.Spec.Storage = &platformv1alpha1.StorageSpec{SizeGi: 20}
	app.Spec.Scheduling.SpreadAcrossZones = true
	app.Spec.Vars = &platformv1alpha1.VarsSpec{SecretRef: varsVolumeName}
	sts := buildStatefulSet(app, "", "deploy-a")
	pod := sts.Spec.Template.Spec
	if len(sts.Spec.VolumeClaimTemplates) != 1 || *pod.AutomountServiceAccountToken || !*pod.SecurityContext.RunAsNonRoot || len(pod.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 2 {
		t.Fatal("incomplete production profile")
	}
	foundVars := false
	for _, v := range pod.Volumes {
		if v.Name == watchVolumeName {
			t.Fatal("PVC shadowed by emptyDir")
		}
		if v.Name == varsVolumeName {
			foundVars = true
		}
	}
	if !foundVars {
		t.Fatal("PVC removed variables volume")
	}
	if !*pod.Containers[0].SecurityContext.ReadOnlyRootFilesystem || pod.Containers[0].StartupProbe == nil {
		t.Fatal("container is not hardened")
	}
}
func TestMetricsSnapshotAndIncompleteFleet(t *testing.T) {
	r, app, sts := productionFixture(t)
	defer fleetMetrics.publish(nil)
	poller := &StatePoller{Client: r.Client, State: r.State}
	if err := poller.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(fleetMetrics)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	complete := false
	for _, family := range families {
		if family.GetName() == "celld_fleet_complete" {
			complete = family.Metric[0].GetGauge().GetValue() == 1
		}
	}
	if !complete {
		t.Fatal("healthy snapshot not complete")
	}
	var pod corev1.Pod
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: app.Namespace, Name: sts.Name + "-0"}, &pod); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
	if err := poller.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	families, err = registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "celld_fleet_complete" && family.Metric[0].GetGauge().GetValue() != 0 {
			t.Fatal("partial fleet marked complete")
		}
	}
}
func TestRemovedRoutesAndAnnotations(t *testing.T) {
	r, app, _ := productionFixture(t)
	app.Spec.Hostnames = []string{testAppHostname}
	ingress := buildIngress(app, "", "issuer")
	if err := r.ensureObject(context.Background(), app, ingress, func(client.Object, client.Object) {}); err != nil {
		t.Fatal(err)
	}
	if err := r.removeObsoleteRoutes(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(ingress), &networkingv1.Ingress{}); err == nil {
		t.Fatal("obsolete ingress survived")
	}
	service := buildPublicService(app)
	reconcileAnnotations(service, map[string]string{"cloud.example/internal": "true"})
	service.Annotations["foreign"] = "foreign-value"
	reconcileAnnotations(service, nil)
	if _, exists := service.Annotations["cloud.example/internal"]; exists || service.Annotations["foreign"] != "foreign-value" {
		t.Fatal("annotation ownership was not preserved")
	}
}
func TestTrackingCredentialRotationAndFailureBackoff(t *testing.T) {
	r, app, _ := productionFixture(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"version":"deploy-a"}`)
	}))
	defer server.Close()
	app.Spec.Bucket.Endpoint = server.URL
	app.Spec.DeployTrackingSecretRef = "tracking"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tracking", Namespace: app.Namespace}, Data: map[string][]byte{"AWS_ACCESS_KEY_ID": []byte("id"), "AWS_SECRET_ACCESS_KEY": []byte("secret")}}
	if err := r.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, _, err := r.pointerVersion(context.Background(), app); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("successful read bypassed cache")
	}
	secret.Data["AWS_SECRET_ACCESS_KEY"] = []byte("rotated")
	if err := r.Update(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.pointerVersion(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("rotation did not invalidate cache")
	}
	app.UID = "new-app"
	if _, _, err := r.pointerVersion(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatal("new resource reused old identity")
	}
	server.Close()
	app.Spec.Bucket.Endpoint = "http://127.0.0.1:1"
	for range 2 {
		if version, _, err := r.pointerVersion(context.Background(), app); err == nil || version != "" {
			t.Fatal("new bucket failure reused stale version")
		}
	}
	app.Spec.DeployTrackingSecretRef = ""
	if _, err := r.currentBucketVersion(context.Background(), app); err == nil {
		t.Fatal("ambient credentials accepted")
	}
}
func TestScaleQueriesRequireFreshCompleteData(t *testing.T) {
	_, app, _ := productionFixture(t)
	app.Spec.Autoscaling = &platformv1alpha1.AutoscalingSpec{Enabled: true, Targets: platformv1alpha1.AutoscalingTargets{P95LatencyMs: ptr.To(int32(100))}}
	object := buildScaledObject(app, "http://prometheus", false)
	triggers, _, err := unstructured.NestedSlice(object.Object, "spec", "triggers")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range triggers {
		metadata := item.(map[string]any)["metadata"].(map[string]any)
		query := metadata["query"].(string)
		if metadata["ignoreNullValues"] != "false" || !strings.Contains(query, "celld_fleet_complete") || !strings.Contains(query, "time()") {
			t.Fatal("unsafe scaler query")
		}
		if strings.Contains(query, "histogram_quantile") && !strings.Contains(query, "destination_service_namespace") {
			t.Fatal("latency query crosses namespaces")
		}
	}
}

func TestProductionConfigurationValidation(t *testing.T) {
	r, base, _ := productionFixture(t)
	for _, host := range []string{testAppHostname, "*.example.com"} {
		app := base.DeepCopy()
		app.Spec.Hostnames = []string{host}
		if err := r.validateApp(app); err != nil {
			t.Fatalf("valid host %s: %v", host, err)
		}
	}
	cases := map[string]func(*platformv1alpha1.WorkerApp){
		"hostname":         func(a *platformv1alpha1.WorkerApp) { a.Spec.Hostnames = []string{"https://example.com"} },
		"image":            func(a *platformv1alpha1.WorkerApp) { a.Spec.Celld.Image = "ghcr.io/denoland/celld:latest" },
		"untrusted image":  func(a *platformv1alpha1.WorkerApp) { a.Spec.Celld.Image = "example.com/celld:v0.4.0" },
		"ambient tracking": func(a *platformv1alpha1.WorkerApp) { a.Spec.AppVersion = AppVersionAuto },
		"endpoint":         func(a *platformv1alpha1.WorkerApp) { a.Spec.Bucket.Endpoint = "https://untrusted.example" },
		"credentials": func(a *platformv1alpha1.WorkerApp) {
			a.Spec.Bucket.CredentialsFrom.IAMRole = "auto"
			a.Spec.Bucket.CredentialsFrom.SecretRef = "secret"
		},
		"ephemeral fleet": func(a *platformv1alpha1.WorkerApp) { a.Spec.Durability = platformv1alpha1.DurabilityFleet },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			app := base.DeepCopy()
			mutate(app)
			if err := r.validateApp(app); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestPrunedHTTPRouteRetryDoesNotRewrite(t *testing.T) {
	r, app, _ := productionFixture(t)
	r.HTTPRouteRetries = true
	r.GatewayName, r.GatewayNamespace = "edge", "infra"
	app.Spec.Hostnames = []string{testAppHostname}
	route := buildHTTPRoute(app, r.GatewayName, r.GatewayNamespace)
	route.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.SchemeGroupVersion.String(), Kind: testWorkerAppKind, Name: app.Name, UID: app.UID, Controller: ptr.To(true)}}
	for i := range route.Spec.Rules {
		route.Spec.Rules[i].Retry = nil
	}
	reconcileAnnotations(route, map[string]string{"celld-operator.io/retry-requested": annotationTrue})
	if err := r.Create(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	var conditions []metav1.Condition
	if retry := r.ensureHTTPRoute(context.Background(), app, &conditions); retry {
		t.Fatal("unexpected route error")
	}
	var live gatewayv1.HTTPRoute
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(route), &live); err != nil {
		t.Fatal(err)
	}
	if live.ResourceVersion != route.ResourceVersion || live.Spec.Rules[0].Retry != nil {
		t.Fatal("pruned retry caused a write")
	}
	if len(conditions) != 1 || conditions[0].Status != metav1.ConditionFalse {
		t.Fatal("missing unsupported retry condition")
	}
}
