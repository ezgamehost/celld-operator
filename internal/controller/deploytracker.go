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
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
)

// Deploy tracking closes the loop between `celld deploy` and the rollout:
// with spec.appVersion "auto", the operator follows the fleet bucket's
// deploy/current.json — the same pointer celld nodes read at startup — and
// a new publication becomes an ordinary gated rollout with no CR edit.
// With a pinned appVersion, the same read powers a mismatch warning, since
// nodes always load what current.json names, not what the CR says.

// AppVersionAuto is the spec.appVersion sentinel that enables tracking.
const AppVersionAuto = "auto"

// deployPointer mirrors celld's protocol.rs DeployPointer (the fields we
// read).
type deployPointer struct {
	Version string `json:"version"`
}

type trackedVersion struct {
	version   string
	fetchedAt time.Time
}

// DeployTracker caches per-fleet bucket-pointer reads so reconciles don't
// hammer the object store; Interval is the poll cadence.
type DeployTracker struct {
	Interval time.Duration

	mu    sync.Mutex
	cache map[types.NamespacedName]trackedVersion
}

func NewDeployTracker(interval time.Duration) *DeployTracker {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	return &DeployTracker{Interval: interval, cache: map[types.NamespacedName]trackedVersion{}}
}

// currentBucketVersion reads deploy/current.json from the fleet's bucket
// using the fleet's own credentials (static keys from the secretRef, or the
// operator's ambient AWS chain otherwise).
func (r *WorkerAppReconciler) currentBucketVersion(ctx context.Context, app *platformv1alpha1.WorkerApp) (string, error) {
	spec := app.Spec.Bucket
	rest, ok := strings.CutPrefix(spec.Name, "s3://")
	if !ok {
		return "", fmt.Errorf("deploy tracking supports s3:// buckets only (got %s)", spec.Name)
	}
	bucket, prefix, _ := strings.Cut(rest, "/")
	key := "deploy/current.json"
	if prefix != "" {
		key = strings.TrimSuffix(prefix, "/") + "/" + key
	}

	region := spec.Region
	if region == "" {
		region = defaultBucketRegion
	}
	loadOptions := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if ref := spec.CredentialsFrom.SecretRef; ref != "" {
		var secret corev1.Secret
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: ref}, &secret); err != nil {
			return "", fmt.Errorf("reading bucket credentials %s: %w", ref, err)
		}
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				string(secret.Data["AWS_ACCESS_KEY_ID"]),
				string(secret.Data["AWS_SECRET_ACCESS_KEY"]),
				string(secret.Data["AWS_SESSION_TOKEN"]),
			)))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return "", err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if spec.Endpoint != "" {
			o.BaseEndpoint = aws.String(spec.Endpoint)
			o.UsePathStyle = true
		}
	})

	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	var pointer deployPointer
	if err := json.NewDecoder(out.Body).Decode(&pointer); err != nil {
		return "", fmt.Errorf("decoding %s: %w", key, err)
	}
	if pointer.Version == "" {
		return "", fmt.Errorf("%s has no version", key)
	}
	return pointer.Version, nil
}

// pointerVersion returns the bucket's current version through the tracker
// cache. stale reports whether the value is a previous read served past its
// TTL because the fresh read failed.
func (r *WorkerAppReconciler) pointerVersion(ctx context.Context, app *platformv1alpha1.WorkerApp) (version string, stale bool, err error) {
	key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
	tracker := r.Deploys

	tracker.mu.Lock()
	cached, have := tracker.cache[key]
	tracker.mu.Unlock()
	if have && time.Since(cached.fetchedAt) < tracker.Interval {
		return cached.version, false, nil
	}

	fresh, err := r.currentBucketVersion(ctx, app)
	if err != nil {
		if have {
			return cached.version, true, err
		}
		return "", false, err
	}
	tracker.mu.Lock()
	tracker.cache[key] = trackedVersion{version: fresh, fetchedAt: time.Now()}
	tracker.mu.Unlock()
	return fresh, false, nil
}

// resolveAppVersion turns spec.appVersion into the concrete version the
// fleet should serve. tracking reports auto mode (the caller shortens its
// requeue to the poll cadence).
func (r *WorkerAppReconciler) resolveAppVersion(ctx context.Context, app *platformv1alpha1.WorkerApp, conditions *[]metav1.Condition) (version string, tracking bool) {
	if app.Spec.AppVersion != AppVersionAuto {
		// Pinned. Best-effort drift warning when the fleet has readable
		// static credentials: nodes load what current.json names, so a
		// pinned version that disagrees with the bucket is a lie waiting
		// to be served.
		if app.Spec.Bucket.CredentialsFrom.SecretRef != "" {
			if current, _, err := r.pointerVersion(ctx, app); err == nil && current != app.Spec.AppVersion {
				*conditions = append(*conditions, metav1.Condition{
					Type: condDeployTrackingReady, Status: metav1.ConditionFalse,
					Reason:  "VersionMismatch",
					Message: fmt.Sprintf("bucket deploy/current.json is %s but spec.appVersion is %s; nodes load the bucket's version", current, app.Spec.AppVersion),
				})
			}
		}
		return app.Spec.AppVersion, false
	}

	current, staleCache, err := r.pointerVersion(ctx, app)
	switch {
	case err == nil:
		*conditions = append(*conditions, metav1.Condition{
			Type: condDeployTrackingReady, Status: metav1.ConditionTrue,
			Reason: "Tracking", Message: "following " + current,
		})
		return current, true
	case staleCache:
		*conditions = append(*conditions, metav1.Condition{
			Type: condDeployTrackingReady, Status: metav1.ConditionFalse,
			Reason:  "BucketUnreachable",
			Message: fmt.Sprintf("holding last known version %s: %v", current, err),
		})
		return current, true
	default:
		// Never seen the pointer: fall back to whatever the live fleet
		// already serves so an unreachable bucket cannot trigger a
		// rollout to nowhere.
		*conditions = append(*conditions, metav1.Condition{
			Type: condDeployTrackingReady, Status: metav1.ConditionFalse,
			Reason: "BucketUnreachable", Message: err.Error(),
		})
		return r.liveTemplateVersion(ctx, app), true
	}
}

func (r *WorkerAppReconciler) liveTemplateVersion(ctx context.Context, app *platformv1alpha1.WorkerApp) string {
	var sts appsv1.StatefulSet
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: fleetName(app)}, &sts); err != nil {
		return ""
	}
	return sts.Spec.Template.Annotations[appVersionAnnotation]
}
