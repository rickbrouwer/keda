/*
Copyright 2023 The KEDA Authors

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

package v1alpha1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	metricscollector "github.com/kedacore/keda/v2/pkg/metricscollector/webhook"
)

var scaledobjectlog = logf.Log.WithName("scaledobject-validation-webhook")

var kc client.Client
var cacheMissToDirectClient bool
var directClient client.Client
var restMapper meta.RESTMapper

var memoryString = "memory"
var cpuString = "cpu"

func (so *ScaledObject) SetupWebhookWithManager(mgr ctrl.Manager, cacheMissFallback bool) error {
	err := setupKubernetesClients(mgr, cacheMissFallback)
	if err != nil {
		return fmt.Errorf("failed to setup kubernetes clients: %w", err)
	}

	return ctrl.NewWebhookManagedBy(mgr).
		WithValidator(&ScaledObjectCustomValidator{}).
		For(so).
		Complete()
}

func setupKubernetesClients(mgr ctrl.Manager, cacheMissFallback bool) error {
	kc = mgr.GetClient()
	restMapper = mgr.GetRESTMapper()
	cacheMissToDirectClient = cacheMissFallback
	if cacheMissToDirectClient {
		cfg := mgr.GetConfig()
		opts := client.Options{
			HTTPClient: mgr.GetHTTPClient(),
			Scheme:     mgr.GetScheme(),
			Mapper:     restMapper,
			Cache:      nil, // this disables the cache and explicitly uses the direct client
		}
		var err error
		directClient, err = client.New(cfg, opts)
		if err != nil {
			return fmt.Errorf("failed to initialize direct client: %w", err)
		}
	}

	return nil
}

// +kubebuilder:webhook:path=/validate-keda-sh-v1alpha1-scaledobject,mutating=false,failurePolicy=ignore,sideEffects=None,groups=keda.sh,resources=scaledobjects,verbs=create;update,versions=v1alpha1,name=vscaledobject.kb.io,admissionReviewVersions=v1

// ScaledObjectCustomValidator is a custom validator for ScaledObject objects
type ScaledObjectCustomValidator struct{}

func (socv ScaledObjectCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	request, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get admission request from context: %w", err)
	}
	so := obj.(*ScaledObject)
	return so.ValidateCreate(request.DryRun)
}

func (socv ScaledObjectCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (warnings admission.Warnings, err error) {
	request, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get admission request from context: %w", err)
	}
	so := newObj.(*ScaledObject)
	old := oldObj.(*ScaledObject)
	return so.ValidateUpdate(old, request.DryRun)
}

func (socv ScaledObjectCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	request, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get admission request from context: %w", err)
	}
	so := obj.(*ScaledObject)
	return so.ValidateDelete(request.DryRun)
}

var _ webhook.CustomValidator = &ScaledObjectCustomValidator{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (so *ScaledObject) ValidateCreate(dryRun *bool) (admission.Warnings, error) {
	val, _ := json.MarshalIndent(so, "", "  ")
	scaledobjectlog.V(1).Info("validating scaledobject creation", 
		"name", so.Name, 
		"namespace", so.Namespace,
		"spec", string(val))
	return validateWorkload(so, "create", *dryRun)
}

func (so *ScaledObject) ValidateUpdate(old runtime.Object, dryRun *bool) (admission.Warnings, error) {
	val, _ := json.MarshalIndent(so, "", "  ")
	scaledobjectlog.V(1).Info("validating scaledobject update", 
		"name", so.Name, 
		"namespace", so.Namespace,
		"spec", string(val))

	if isRemovingFinalizer(so, old) {
		scaledobjectlog.V(1).Info("finalizer removal detected, skipping validation", 
			"name", so.Name, 
			"namespace", so.Namespace)
		return nil, nil
	}

	return validateWorkload(so, "update", *dryRun)
}

func (so *ScaledObject) ValidateDelete(_ *bool) (admission.Warnings, error) {
	scaledobjectlog.V(1).Info("validating scaledobject deletion", 
		"name", so.Name, 
		"namespace", so.Namespace)
	return nil, nil
}

func isRemovingFinalizer(so *ScaledObject, old runtime.Object) bool {
	oldSo := old.(*ScaledObject)

	soSpec, _ := json.MarshalIndent(so.Spec, "", "  ")
	oldSoSpec, _ := json.MarshalIndent(oldSo.Spec, "", "  ")
	soSpecString := string(soSpec)
	oldSoSpecString := string(oldSoSpec)

	return len(so.ObjectMeta.Finalizers) < len(oldSo.ObjectMeta.Finalizers) && soSpecString == oldSoSpecString
}

// validateWorkload runs all validations in parallel for better performance
func validateWorkload(so *ScaledObject, action string, dryRun bool) (admission.Warnings, error) {
	metricscollector.RecordScaledObjectValidatingTotal(so.Namespace, action)

	type validationResult struct {
		name string
		err  error
	}

	// Define validations with their names for better logging
	validations := []struct {
		name string
		fn   func(*ScaledObject, string, bool) error
	}{
		{"verifyCPUMemoryScalers", verifyCPUMemoryScalers},
		{"verifyScaledObjects", verifyScaledObjects},
		{"verifyHpas", verifyHpas},
		{"verifyReplicaCount", verifyReplicaCount},
		{"verifyFallback", verifyFallback},
	}

	// Channel for collecting validation results
	resultChan := make(chan validationResult, len(validations))
	var wg sync.WaitGroup

	// Start all validations in parallel
	for _, validation := range validations {
		wg.Add(1)
		go func(name string, fn func(*ScaledObject, string, bool) error) {
			defer wg.Done()
			scaledobjectlog.V(1).Info("starting validation", 
				"function", name, 
				"scaledobject", so.Name,
				"namespace", so.Namespace)
			
			err := fn(so, action, dryRun)
			resultChan <- validationResult{name: name, err: err}
			
			if err != nil {
				scaledobjectlog.Error(err, "validation failed", 
					"function", name, 
					"scaledobject", so.Name,
					"namespace", so.Namespace)
			} else {
				scaledobjectlog.V(1).Info("validation completed successfully", 
					"function", name, 
					"scaledobject", so.Name,
					"namespace", so.Namespace)
			}
		}(validation.name, validation.fn)
	}

	// Wait for all validations to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results - return first error encountered
	for result := range resultChan {
		if result.err != nil {
			return nil, fmt.Errorf("validation '%s' failed: %w", result.name, result.err)
		}
	}

	// Run trigger validation (kept separate as it's used by both ScaledObject and ScaledJob)
	scaledobjectlog.V(1).Info("running trigger validation", 
		"scaledobject", so.Name,
		"namespace", so.Namespace)
	
	if err := verifyTriggers(so, action, dryRun); err != nil {
		return nil, fmt.Errorf("trigger validation failed: %w", err)
	}

	scaledobjectlog.V(1).Info("scaledobject validation completed successfully", 
		"name", so.Name,
		"namespace", so.Namespace)
	return nil, nil
}

func verifyReplicaCount(incomingSo *ScaledObject, action string, _ bool) error {
	err := CheckReplicaCountBoundsAreValid(incomingSo)
	if err != nil {
		scaledobjectlog.Error(err, "replica count validation failed", 
			"name", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		metricscollector.RecordScaledObjectValidatingErrors(incomingSo.Namespace, action, "incorrect-replicas")
		return fmt.Errorf("replica count validation failed: %w", err)
	}
	return nil
}

func verifyFallback(incomingSo *ScaledObject, action string, _ bool) error {
	err := CheckFallbackValid(incomingSo)
	if err != nil {
		scaledobjectlog.Error(err, "fallback validation failed", 
			"name", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		metricscollector.RecordScaledObjectValidatingErrors(incomingSo.Namespace, action, "incorrect-fallback")
		return fmt.Errorf("fallback validation failed: %w", err)
	}
	return nil
}

func verifyTriggers(incomingObject interface{}, action string, _ bool) error {
	var triggers []ScaleTriggers
	var name string
	var namespace string
	
	switch obj := incomingObject.(type) {
	case *ScaledObject:
		triggers = obj.Spec.Triggers
		name = obj.Name
		namespace = obj.Namespace
	case *ScaledJob:
		triggers = obj.Spec.Triggers
		name = obj.Name
		namespace = obj.Namespace
	default:
		return fmt.Errorf("unknown scalable object type %T", incomingObject)
	}

	err := ValidateTriggers(triggers)
	if err != nil {
		scaledobjectlog.Error(err, "trigger validation failed", 
			"name", name,
			"namespace", namespace,
			"triggerCount", len(triggers))
		metricscollector.RecordScaledObjectValidatingErrors(namespace, action, "incorrect-triggers")
		return fmt.Errorf("trigger validation failed: %w", err)
	}
	return nil
}

// verifyHpas uses field selectors for more efficient queries
func verifyHpas(incomingSo *ScaledObject, action string, _ bool) error {
	// Early return if no scale target
	if incomingSo.Spec.ScaleTargetRef.Name == "" {
		scaledobjectlog.V(1).Info("no scale target name specified, skipping HPA validation", 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		return nil
	}

	incomingSoGvkr, err := ParseGVKR(restMapper, incomingSo.Spec.ScaleTargetRef.APIVersion, incomingSo.Spec.ScaleTargetRef.Kind)
	if err != nil {
		scaledobjectlog.Error(err, "failed to parse GVKR from incoming ScaledObject", 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace,
			"apiVersion", incomingSo.Spec.ScaleTargetRef.APIVersion, 
			"kind", incomingSo.Spec.ScaleTargetRef.Kind)
		return fmt.Errorf("failed to parse GVKR from incoming ScaledObject: %w", err)
	}

	// Use field selector to target HPAs by scale target name for efficiency
	fieldSelector := fmt.Sprintf("spec.scaleTargetRef.name=%s", incomingSo.Spec.ScaleTargetRef.Name)
	hpaList := &autoscalingv2.HorizontalPodAutoscalerList{}
	opt := &client.ListOptions{
		Namespace:     incomingSo.Namespace,
		FieldSelector: fields.ParseSelectorOrDie(fieldSelector),
	}
	
	if err := kc.List(context.Background(), hpaList, opt); err != nil {
		scaledobjectlog.Error(err, "failed to list HPAs", 
			"namespace", incomingSo.Namespace,
			"fieldSelector", fieldSelector)
		return fmt.Errorf("failed to list HPAs in namespace %s: %w", incomingSo.Namespace, err)
	}

	scaledobjectlog.V(1).Info("found HPAs for validation", 
		"count", len(hpaList.Items),
		"scaledobject", incomingSo.Name,
		"namespace", incomingSo.Namespace)

	for i := range hpaList.Items {
		hpa := &hpaList.Items[i]
		
		// Skip if ownership validation is disabled
		if hpa.ObjectMeta.Annotations[ValidationsHpaOwnershipAnnotation] == "false" {
			scaledobjectlog.V(1).Info("skipping HPA validation due to annotation", 
				"hpa", hpa.Name,
				"annotation", ValidationsHpaOwnershipAnnotation)
			continue
		}

		if err := validateHpaConflict(hpa, incomingSo, incomingSoGvkr, action); err != nil {
			return err
		}
	}
	
	return nil
}

func validateHpaConflict(hpa *autoscalingv2.HorizontalPodAutoscaler, incomingSo *ScaledObject, incomingSoGvkr GroupVersionKindResource, action string) error {
	hpaGvkr, err := ParseGVKR(restMapper, hpa.Spec.ScaleTargetRef.APIVersion, hpa.Spec.ScaleTargetRef.Kind)
	if err != nil {
		scaledobjectlog.Error(err, "failed to parse GVKR from HPA", 
			"hpa", hpa.Name,
			"namespace", hpa.Namespace,
			"apiVersion", hpa.Spec.ScaleTargetRef.APIVersion, 
			"kind", hpa.Spec.ScaleTargetRef.Kind)
		return fmt.Errorf("failed to parse GVKR from HPA %s: %w", hpa.Name, err)
	}

	// Only check HPAs that target the same resource type
	if hpaGvkr.GVKString() != incomingSoGvkr.GVKString() {
		return nil
	}

	// Check if HPA is owned by this ScaledObject
	for i := range hpa.OwnerReferences {
		owner := &hpa.OwnerReferences[i]
		if owner.Kind == incomingSo.Kind && owner.Name == incomingSo.Name {
			scaledobjectlog.V(1).Info("HPA is owned by this ScaledObject", 
				"hpa", hpa.Name,
				"scaledobject", incomingSo.Name)
			return nil // Owned by this ScaledObject
		}
	}

	// Check for ownership transfer
	if incomingSo.ObjectMeta.Annotations[ScaledObjectTransferHpaOwnershipAnnotation] == "true" &&
		incomingSo.Spec.Advanced != nil &&
		incomingSo.Spec.Advanced.HorizontalPodAutoscalerConfig != nil &&
		incomingSo.Spec.Advanced.HorizontalPodAutoscalerConfig.Name == hpa.Name {
		scaledobjectlog.Info("HPA ownership transfer detected", 
			"hpa", hpa.Name, 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		return nil
	}

	// Conflict detected
	err = fmt.Errorf("workload '%s' of type '%s' is already managed by HPA '%s'", 
		incomingSo.Spec.ScaleTargetRef.Name, incomingSoGvkr.GVKString(), hpa.Name)
	
	scaledobjectlog.Error(err, "HPA conflict detected",
		"scaledobject", incomingSo.Name,
		"namespace", incomingSo.Namespace,
		"hpa", hpa.Name,
		"workload", incomingSo.Spec.ScaleTargetRef.Name,
		"workloadType", incomingSoGvkr.GVKString())
	
	metricscollector.RecordScaledObjectValidatingErrors(incomingSo.Namespace, action, "other-hpa")
	return err
}

func verifyScaledObjects(incomingSo *ScaledObject, action string, _ bool) error {
	// Early return if no scale target
	if incomingSo.Spec.ScaleTargetRef.Name == "" {
		scaledobjectlog.V(1).Info("no scale target name specified, skipping ScaledObject validation", 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		return nil
	}

	incomingSoGvkr, err := ParseGVKR(restMapper, incomingSo.Spec.ScaleTargetRef.APIVersion, incomingSo.Spec.ScaleTargetRef.Kind)
	if err != nil {
		scaledobjectlog.Error(err, "failed to parse GVKR from incoming ScaledObject",
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace,
			"apiVersion", incomingSo.Spec.ScaleTargetRef.APIVersion,
			"kind", incomingSo.Spec.ScaleTargetRef.Kind)
		return fmt.Errorf("failed to parse GVKR from incoming ScaledObject: %w", err)
	}

	soList := &ScaledObjectList{}
	opt := &client.ListOptions{
		Namespace: incomingSo.Namespace,
	}
	
	if err := kc.List(context.Background(), soList, opt); err != nil {
		scaledobjectlog.Error(err, "failed to list ScaledObjects",
			"namespace", incomingSo.Namespace)
		return fmt.Errorf("failed to list ScaledObjects in namespace %s: %w", incomingSo.Namespace, err)
	}

	scaledobjectlog.V(1).Info("found ScaledObjects for validation", 
		"count", len(soList.Items),
		"scaledobject", incomingSo.Name,
		"namespace", incomingSo.Namespace)

	incomingSoHpaName := getHpaName(*incomingSo)
	
	for i := range soList.Items {
		so := &soList.Items[i]
		
		// Skip self
		if so.Name == incomingSo.Name {
			continue
		}

		if err := validateScaledObjectConflict(so, incomingSo, incomingSoGvkr, incomingSoHpaName, action); err != nil {
			return err
		}
	}

	// Validate scaling modifiers if present
	if incomingSo.IsUsingModifiers() {
		scaledobjectlog.V(1).Info("validating scaling modifiers", 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		
		if _, err := ValidateAndCompileScalingModifiers(incomingSo); err != nil {
			scaledobjectlog.Error(err, "failed to validate scaling modifiers",
				"scaledobject", incomingSo.Name,
				"namespace", incomingSo.Namespace)
			metricscollector.RecordScaledObjectValidatingErrors(incomingSo.Namespace, action, "scaling-modifiers")
			return fmt.Errorf("failed to validate scaling modifiers: %w", err)
		}
	}

	return nil
}

func validateScaledObjectConflict(existingSo, incomingSo *ScaledObject, incomingSoGvkr GroupVersionKindResource, incomingSoHpaName, action string) error {
	soGvkr, err := ParseGVKR(restMapper, existingSo.Spec.ScaleTargetRef.APIVersion, existingSo.Spec.ScaleTargetRef.Kind)
	if err != nil {
		scaledobjectlog.Error(err, "failed to parse GVKR from existing ScaledObject",
			"scaledobject", existingSo.Name,
			"namespace", existingSo.Namespace,
			"apiVersion", existingSo.Spec.ScaleTargetRef.APIVersion,
			"kind", existingSo.Spec.ScaleTargetRef.Kind)
		return fmt.Errorf("failed to parse GVKR from ScaledObject %s: %w", existingSo.Name, err)
	}

	// Check workload conflict
	if soGvkr.GVKString() == incomingSoGvkr.GVKString() &&
		existingSo.Spec.ScaleTargetRef.Name == incomingSo.Spec.ScaleTargetRef.Name {
		
		err := fmt.Errorf("workload '%s' of type '%s' is already managed by ScaledObject '%s'", 
			existingSo.Spec.ScaleTargetRef.Name, incomingSoGvkr.GVKString(), existingSo.Name)
		
		scaledobjectlog.Error(err, "ScaledObject workload conflict",
			"existing_scaledobject", existingSo.Name,
			"incoming_scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace,
			"workload", existingSo.Spec.ScaleTargetRef.Name,
			"workload_type", incomingSoGvkr.GVKString())
		
		metricscollector.RecordScaledObjectValidatingErrors(incomingSo.Namespace, action, "other-scaled-object")
		return err
	}

	// Check HPA name conflict
	if getHpaName(*existingSo) == incomingSoHpaName {
		err := fmt.Errorf("HPA '%s' is already managed by ScaledObject '%s'", 
			incomingSoHpaName, existingSo.Name)
		
		scaledobjectlog.Error(err, "ScaledObject HPA conflict",
			"existing_scaledobject", existingSo.Name,
			"incoming_scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace,
			"hpa_name", incomingSoHpaName)
		
		metricscollector.RecordScaledObjectValidatingErrors(incomingSo.Namespace, action, "other-scaled-object-hpa")
		return err
	}

	return nil
}

func getFromCacheOrDirect(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	err := kc.Get(ctx, key, obj, &client.GetOptions{})
	if cacheMissToDirectClient {
		if kerrors.IsNotFound(err) {
			return directClient.Get(ctx, key, obj, &client.GetOptions{})
		}
	}
	return err
}

// verifyCPUMemoryScalers with early returns and optimizations
func verifyCPUMemoryScalers(incomingSo *ScaledObject, action string, dryRun bool) error {
	if dryRun {
		return nil
	}

	// Early return if no scale target
	if incomingSo.Spec.ScaleTargetRef.Name == "" {
		scaledobjectlog.V(1).Info("no scale target name specified, skipping CPU/Memory validation", 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		return nil
	}

	// Early return if no CPU/memory triggers
	hasCPUMemoryTriggers := false
	var cpuMemoryTriggers []ScaleTriggers
	
	for i := range incomingSo.Spec.Triggers {
		trigger := &incomingSo.Spec.Triggers[i]
		if trigger.Type == cpuString || trigger.Type == memoryString {
			hasCPUMemoryTriggers = true
			cpuMemoryTriggers = append(cpuMemoryTriggers, *trigger)
		}
	}
	
	if !hasCPUMemoryTriggers {
		scaledobjectlog.V(1).Info("no CPU/Memory triggers found, skipping validation", 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		return nil
	}

	scaledobjectlog.V(1).Info("found CPU/Memory triggers for validation", 
		"count", len(cpuMemoryTriggers),
		"scaledobject", incomingSo.Name,
		"namespace", incomingSo.Namespace)

	// Parse GVKR only when needed
	incomingSoGvkr, err := ParseGVKR(restMapper, incomingSo.Spec.ScaleTargetRef.APIVersion, incomingSo.Spec.ScaleTargetRef.Kind)
	if err != nil {
		scaledobjectlog.Error(err, "failed to parse GVKR from incoming ScaledObject", 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace,
			"apiVersion", incomingSo.Spec.ScaleTargetRef.APIVersion, 
			"kind", incomingSo.Spec.ScaleTargetRef.Kind)
		return fmt.Errorf("failed to parse GVKR: %w", err)
	}

	// Early return for unsupported workload types
	switch incomingSoGvkr.GVKString() {
	case "apps/v1.Deployment", "apps/v1.StatefulSet":
		// Supported types, continue
	default:
		scaledobjectlog.V(1).Info("unsupported workload type for CPU/Memory validation", 
			"workloadType", incomingSoGvkr.GVKString(),
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace)
		return nil // Unsupported type, skip validation
	}

	podSpec, err := getPodSpec(incomingSo, incomingSoGvkr)
	if err != nil {
		scaledobjectlog.Error(err, "failed to get pod spec for CPU/Memory validation", 
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace,
			"workloadType", incomingSoGvkr.GVKString())
		return fmt.Errorf("failed to get pod spec: %w", err)
	}

	return validateCPUMemoryTriggers(incomingSo, podSpec, cpuMemoryTriggers, action)
}

func getPodSpec(so *ScaledObject, gvkr GroupVersionKindResource) (*corev1.PodSpec, error) {
	key := types.NamespacedName{
		Namespace: so.Namespace,
		Name:      so.Spec.ScaleTargetRef.Name,
	}

	switch gvkr.GVKString() {
	case "apps/v1.Deployment":
		deployment := &appsv1.Deployment{}
		if err := getFromCacheOrDirect(context.Background(), key, deployment); err != nil {
			return nil, fmt.Errorf("failed to get deployment %s: %w", key.Name, err)
		}
		return &deployment.Spec.Template.Spec, nil
		
	case "apps/v1.StatefulSet":
		statefulset := &appsv1.StatefulSet{}
		if err := getFromCacheOrDirect(context.Background(), key, statefulset); err != nil {
			return nil, fmt.Errorf("failed to get statefulset %s: %w", key.Name, err)
		}
		return &statefulset.Spec.Template.Spec, nil
		
	default:
		return nil, fmt.Errorf("unsupported workload type: %s", gvkr.GVKString())
	}
}

func validateCPUMemoryTriggers(incomingSo *ScaledObject, podSpec *corev1.PodSpec, triggers []ScaleTriggers, action string) error {
	scaleToZeroErr := true
	
	for i := range triggers {
		trigger := &triggers[i]
		
		// Check if this trigger breaks the scale-to-zero requirement
		if trigger.Type != cpuString && trigger.Type != memoryString {
			scaleToZeroErr = false
		}
		
		containerName := trigger.Metadata["containerName"]
		found := false
		
		for j := range podSpec.Containers {
			container := &podSpec.Containers[j]
			
			// Skip if container name is specified and doesn't match
			if containerName != "" && container.Name != containerName {
				continue
			}
			
			found = true
			resourceType := corev1.ResourceName(trigger.Type)
			
			// Check if resource is set in workload or in LimitRange
			if !isWorkloadResourceSet(container.Resources, resourceType) &&
				!isContainerResourceLimitSet(context.Background(), incomingSo.Namespace, resourceType) {
				
				err := fmt.Errorf("scaledobject has a %v trigger but container %s doesn't have the %v request defined", 
					resourceType, container.Name, resourceType)
				
				scaledobjectlog.Error(err, "missing resource request/limit",
					"scaledobject", incomingSo.Name,
					"namespace", incomingSo.Namespace,
					"container", container.Name,
					"resourceType", resourceType)
				
				metricscollector.RecordScaledObjectValidatingErrors(incomingSo.Namespace, action, "missing-requests")
				return err
			}
		}
		
		if !found && containerName != "" {
			err := fmt.Errorf("container '%s' not found in pod spec", containerName)
			scaledobjectlog.Error(err, "container not found",
				"scaledobject", incomingSo.Name,
				"namespace", incomingSo.Namespace,
				"containerName", containerName)
			return err
		}
	}

	// Validate scale-to-zero requirements
	if scaleToZeroErr && (incomingSo.Spec.MinReplicaCount == nil || *incomingSo.Spec.MinReplicaCount == 0) {
		err := fmt.Errorf("scaledobject has only cpu/memory triggers AND minReplica is 0 (scale to zero doesn't work in this case)")
		scaledobjectlog.Error(err, "scale-to-zero requirements not met",
			"scaledobject", incomingSo.Name,
			"namespace", incomingSo.Namespace,
			"minReplicaCount", incomingSo.Spec.MinReplicaCount)
		metricscollector.RecordScaledObjectValidatingErrors(incomingSo.Namespace, action, "scale-to-zero-requirements-not-met")
		return err
	}

	return nil
}

func ValidateAndCompileScalingModifiers(so *ScaledObject) (*vm.Program, error) {
	sm := so.Spec.Advanced.ScalingModifiers

	if sm.Formula == "" {
		return nil, fmt.Errorf("ScalingModifiers.Formula is mandatory")
	}

	// Cast return value of formula to float if necessary
	so.Spec.Advanced.ScalingModifiers.Formula = castToFloatIfNecessary(sm.Formula)

	// Validate formula
	compiledFormula, err := validateScalingModifiersFormula(so)
	if err != nil {
		return nil, fmt.Errorf("error validating formula in ScalingModifiers: %w", err)
	}
	
	// Validate target
	if err := validateScalingModifiersTarget(so); err != nil {
		return nil, fmt.Errorf("error validating target in ScalingModifiers: %w", err)
	}
	
	return compiledFormula, nil
}

func validateScalingModifiersFormula(so *ScaledObject) (*vm.Program, error) {
	sm := so.Spec.Advanced.ScalingModifiers

	if sm.Formula == "" {
		return nil, nil
	}
	
	if sm.Target == "" {
		return nil, fmt.Errorf("formula is given but target is empty")
	}

	// Build trigger map with dummy values
	triggersMap := make(map[string]float64)
	dummyValue := -1.0
	
	for i := range so.Spec.Triggers {
		trigger := &so.Spec.Triggers[i]
		// Skip resource metrics
		if trigger.Type == cpuString || trigger.Type == memoryString {
			continue
		}
		if trigger.Name != "" {
			triggersMap[trigger.Name] = dummyValue
		}
	}
	
	compiled, err := expr.Compile(sm.Formula, expr.Env(triggersMap), expr.AsFloat64())
	if err != nil {
		return nil, fmt.Errorf("failed to compile formula: %w", err)
	}
	
	if _, err := expr.Run(compiled, triggersMap); err != nil {
		return nil, fmt.Errorf("failed to run formula with dummy values: %w", err)
	}
	
	return compiled, nil
}

func validateScalingModifiersTarget(so *ScaledObject) error {
	sm := so.Spec.Advanced.ScalingModifiers

	if sm.Target == "" {
		return nil
	}

	num, err := strconv.ParseFloat(sm.Target, 64)
	if err != nil || num <= 0.0 {
		return fmt.Errorf("failed to convert target to valid positive float: %w", err)
	}

	if so.Spec.Advanced.ScalingModifiers.MetricType == autoscalingv2.UtilizationMetricType {
		return fmt.Errorf("metric type cannot be Utilization, must be AverageValue or Value for external metrics")
	}

	return nil
}

func castToFloatIfNecessary(formula string) string {
	if strings.HasPrefix(formula, "float(") {
		return formula
	}
	return "float(" + formula + ")"
}

func isWorkloadResourceSet(rr corev1.ResourceRequirements, name corev1.ResourceName) bool {
	requests, requestsSet := rr.Requests[name]
	limits, limitsSet := rr.Limits[name]
	return (requestsSet || limitsSet) && (requests.AsApproximateFloat64() > 0 || limits.AsApproximateFloat64() > 0)
}

func isContainerResourceSetInLimitRangeObject(item corev1.LimitRangeItem, resourceName corev1.ResourceName) bool {
	request, isRequestSet := item.DefaultRequest[resourceName]
	limit, isLimitSet := item.Default[resourceName]

	return (isRequestSet || isLimitSet) &&
		(request.AsApproximateFloat64() > 0 || limit.AsApproximateFloat64() > 0) &&
		item.Type == corev1.LimitTypeContainer
}

func isContainerResourceLimitSet(ctx context.Context, namespace string, triggerType corev1.ResourceName) bool {
	limitRangeList := &corev1.LimitRangeList{}
	listOps := &client.ListOptions{
		Namespace: namespace,
	}

	if err := kc.List(ctx, limitRangeList, listOps); err != nil {
		scaledobjectlog.Error(err, "failed to list LimitRanges", 
			"namespace", namespace)
		return false
	}

	for i := range limitRangeList.Items {
		limitRange := &limitRangeList.Items[i]
		for j := range limitRange.Spec.Limits {
			limit := &limitRange.Spec.Limits[j]
			if isContainerResourceSetInLimitRangeObject(*limit, triggerType) {
				return true
			}
		}
	}

	scaledobjectlog.V(1).Info("no container limit range found", 
		"namespace", namespace,
		"resourceType", triggerType)
	return false
}

func getHpaName(so ScaledObject) string {
	if so.Spec.Advanced == nil || 
		so.Spec.Advanced.HorizontalPodAutoscalerConfig == nil || 
		so.Spec.Advanced.HorizontalPodAutoscalerConfig.Name == "" {
		return fmt.Sprintf("keda-hpa-%s", so.Name)
	}
	return so.Spec.Advanced.HorizontalPodAutoscalerConfig.Name
}
