package controller

import (
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"

	platformv1alpha1 "github.com/ezgamehost/celld-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation"
)

var runtimeImageTag = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+([-.+][a-zA-Z0-9.-]+)?$`)

// Validate before creating children, including objects admitted by older CRDs.
func (r *WorkerAppReconciler) validateApp(app *platformv1alpha1.WorkerApp) error {
	if len(app.Name) > 48 || len(validation.IsDNS1123Label(app.Name)) > 0 {
		return fmt.Errorf("name must be a DNS label of at most 48 characters")
	}
	for _, host := range app.Spec.Hostnames {
		if len(validation.IsDNS1123Subdomain(host)) > 0 && len(validation.IsWildcardDNS1123Subdomain(host)) > 0 {
			return fmt.Errorf("invalid hostname %q", host)
		}
	}
	if app.Spec.AppVersion == "" {
		return fmt.Errorf("appVersion is required")
	}
	image, _, _ := strings.Cut(app.Spec.Celld.Image, "@")
	_, tag, tagged := strings.Cut(image[strings.LastIndex(image, "/")+1:], ":")
	if !tagged || !runtimeImageTag.MatchString(tag) {
		return fmt.Errorf("celld image must include a numeric major.minor.patch tag, including when digest-pinned")
	}
	creds := app.Spec.Bucket.CredentialsFrom
	if app.Spec.AppVersion == AppVersionAuto && (!trackableBucket(app) || app.Spec.DeployTrackingSecretRef == "") {
		return fmt.Errorf("auto requires an s3 bucket and deployTrackingSecretRef")
	}
	for _, ref := range []string{creds.SecretRef, app.Spec.DeployTrackingSecretRef} {
		if ref != "" && len(validation.IsDNS1123Subdomain(ref)) > 0 {
			return fmt.Errorf("invalid Secret reference")
		}
	}
	if app.Spec.Vars != nil && (app.Spec.Vars.SecretRef == "" || len(validation.IsDNS1123Subdomain(app.Spec.Vars.SecretRef)) > 0) {
		return fmt.Errorf("vars.secretRef is required and must be a Secret name")
	}
	if desiredReplicas(app) < 1 || app.Spec.Resources.MemoryGi < 0 || app.Spec.Resources.MemoryGi > 65536 {
		return fmt.Errorf("invalid replica or memory configuration")
	}
	if as := app.Spec.Autoscaling; as != nil && as.Enabled && defaultPositive(as.MinReplicas, 2) > defaultPositive(as.MaxReplicas, 10) {
		return fmt.Errorf("minReplicas exceeds maxReplicas")
	}
	if app.Spec.Durability == platformv1alpha1.DurabilityFleet && (app.Spec.Storage == nil || desiredReplicas(app) < 3 || (autoscalingEnabled(app) && defaultPositive(app.Spec.Autoscaling.MinReplicas, 2) < 3)) {
		return fmt.Errorf("fleet durability requires persistent storage and at least three replicas")
	}
	if len(validation.IsDNS1123Subdomain(clusterDomain(app))) > 0 {
		return fmt.Errorf("invalid clusterDomain")
	}
	return r.validateTrust(app)
}

func (r *WorkerAppReconciler) validateTrust(app *platformv1alpha1.WorkerApp) error {
	prefixes := r.AllowedImagePrefixes
	if len(prefixes) == 0 {
		prefixes = []string{"ghcr.io/denoland/celld:"}
	}
	allowed := false
	for _, prefix := range prefixes {
		if strings.HasPrefix(app.Spec.Celld.Image, prefix) {
			allowed = true
		}
	}
	if !allowed {
		return fmt.Errorf("celld image is not in the operator image allowlist")
	}
	creds := app.Spec.Bucket.CredentialsFrom
	count := 0
	for _, value := range []string{creds.IAMRole, creds.SecretRef, creds.AzureClientID} {
		if value != "" {
			count++
		}
	}
	if count > 1 {
		return fmt.Errorf("select only one bucket credential family")
	}
	if creds.IAMRole != "" && creds.IAMRole != iamRoleAuto && !slices.Contains(r.AllowedIAMRoles, creds.IAMRole) {
		return fmt.Errorf("IAM role is not in the operator allowlist")
	}
	if creds.AzureClientID != "" && !slices.Contains(r.AllowedAzureClientIDs, creds.AzureClientID) {
		return fmt.Errorf("azure identity is not in the operator allowlist")
	}
	if endpoint := app.Spec.Bucket.Endpoint; endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !slices.Contains(r.AllowedBucketEndpoints, endpoint) {
			return fmt.Errorf("bucket endpoint must be an explicitly allowed HTTPS URL without credentials, query, or fragment")
		}
	}
	return nil
}
