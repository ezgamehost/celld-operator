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
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
)

const testStatefulSetKind = "StatefulSet"

func TestOwnershipCollisions(t *testing.T) {
	for _, ownedByOther := range []bool{false, true} {
		for _, kind := range []string{testStatefulSetKind, "ScaledObject"} {
			t.Run(kind+"/foreign="+strconv.FormatBool(ownedByOther), func(t *testing.T) {
				scheme := runtime.NewScheme()
				for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, platformv1alpha1.AddToScheme} {
					if err := add(scheme); err != nil {
						t.Fatal(err)
					}
				}
				app := &platformv1alpha1.WorkerApp{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default", UID: "app-uid"}}
				app.Spec.Celld.Image = testCelldImage
				app.Spec.Bucket.Name = "s3://bucket/app"
				var obj client.Object
				if kind == testStatefulSetKind {
					obj = buildStatefulSet(app, "", "old")
				} else {
					obj = newUnstructuredObject("keda.sh/v1alpha1", kind, fleetName(app), app.Namespace, nil, nil, map[string]any{"sentinel": "preserve"})
				}
				if ownedByOther {
					obj.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: platformv1alpha1.SchemeGroupVersion.String(), Kind: testWorkerAppKind, Name: app.Name, UID: "previous-app-uid", Controller: ptr.To(true)}})
				}
				c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()
				r := &WorkerAppReconciler{Client: c, Reader: c, Scheme: scheme}
				var err error
				if kind == testStatefulSetKind {
					_, err = r.reconcileFleet(context.Background(), app, "new")
				} else {
					desired := obj.DeepCopyObject().(*unstructured.Unstructured)
					desired.Object["spec"] = map[string]any{"changed": true}
					err = r.ensureUnstructured(context.Background(), app, desired)
					var conditions []metav1.Condition
					if !r.ensureScaledObject(context.Background(), app, fleetOutcome{}, &conditions) || len(conditions) != 1 {
						t.Fatal("cleanup must report an ownership conflict")
					}
				}
				if err == nil || !strings.Contains(err.Error(), "not controlled by WorkerApp") {
					t.Fatalf("expected ownership error, got %v", err)
				}
				live := obj.DeepCopyObject().(client.Object)
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), live); err != nil {
					t.Fatal(err)
				}
				if kind == testStatefulSetKind && live.(*appsv1.StatefulSet).Spec.Template.Annotations[appVersionAnnotation] != "old" {
					t.Fatal("foreign StatefulSet was changed")
				}
				if kind == "ScaledObject" && live.(*unstructured.Unstructured).Object["spec"].(map[string]any)["sentinel"] != "preserve" {
					t.Fatal("foreign ScaledObject was changed")
				}
			})
		}
	}
}

type stateRoundTripper func(*http.Request) (*http.Response, error)

func (f stateRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStateClientPodAddresses(t *testing.T) {
	for _, address := range []struct{ ip, host string }{{"10.0.0.1", "10.0.0.1:8081"}, {"fd00::1", "[fd00::1]:8081"}} {
		t.Run(address.ip, func(t *testing.T) {
			c := &StateClient{HTTP: &http.Client{Transport: stateRoundTripper(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host != address.host || req.URL.Path != "/state" {
					t.Fatalf("unexpected URL %s", req.URL)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"occupied":2,"restoring":1,"shedding":null}`)), Header: make(http.Header)}, nil
			})}}
			state, err := c.Fetch(context.Background(), address.ip)
			if err != nil {
				t.Fatal(err)
			}
			if state.Occupied != 2 || state.Restoring != 1 {
				t.Fatalf("unexpected state: %+v", state)
			}
		})
	}
}

func TestStringMapSubsetRequiresKey(t *testing.T) {
	if stringMapSubset(map[string]string{"key": ""}, nil) {
		t.Fatal("missing key is not an empty-valued key")
	}
}
