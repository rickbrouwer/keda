/*
Copyright 2022 The KEDA Authors

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

package fallback

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/go-logr/logr"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/scale"
	"k8s.io/metrics/pkg/apis/external_metrics"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scaling/resolver"
)

var log = logf.Log.WithName("fallback")

type FallbackResult struct {
	ShouldFallback   bool
	Replicas         int32
	Metrics          []external_metrics.ExternalMetricValue
	OriginalError    error
}

func HandleFallback(ctx context.Context, client runtimeclient.Client, scaleClient scale.ScalesGetter, logger logr.Logger,
	scaledObject *kedav1alpha1.ScaledObject, isError bool, metricSpec v2.MetricSpec) (*FallbackResult, error) {
	if !shouldEnableFallback(scaledObject, metricSpec) {
		return &FallbackResult{ShouldFallback: false}, nil
	}

	status := scaledObject.Status.DeepCopy()
	initHealthStatus(status)

	// Update health status for all metrics
	for metricName := range status.Health {
		healthStatus := getHealthStatus(status, metricName)
		if isError {
			healthStatus.Status = kedav1alpha1.HealthStatusFailing
			*healthStatus.NumberOfFailures++
		} else {
			healthStatus.Status = kedav1alpha1.HealthStatusHappy
			zero := int32(0)
			healthStatus.NumberOfFailures = &zero
		}
		status.Health[metricName] = *healthStatus
	}

	updateStatus(ctx, client, scaledObject, status, metricSpec)

	if !hasExceededFailureThreshold(status, scaledObject.Spec.Fallback.FailureThreshold) {
		return &FallbackResult{ShouldFallback: false}, nil
	}

	currentReplicas, err := resolver.GetCurrentReplicas(ctx, client, scaleClient, scaledObject)
	if err != nil {
		return nil, fmt.Errorf("error getting current replicas: %w", err)
	}

	fallbackReplicas := calculateFallbackReplicas(scaledObject, currentReplicas)
	fallbackMetrics := calculateFallbackMetrics(scaledObject, metricSpec, fallbackReplicas)

	logger.Info("Using fallback configuration",
		"scaledObject.Namespace", scaledObject.Namespace,
		"scaledObject.Name", scaledObject.Name,
		"fallback.behavior", scaledObject.Spec.Fallback.Behavior,
		"fallback.replicas", fallbackReplicas,
		"currentReplicas", currentReplicas)

	return &FallbackResult{
		ShouldFallback: true,
		Replicas:       fallbackReplicas,
		Metrics:        fallbackMetrics,
		OriginalError:  err,
	}, nil
}

func shouldEnableFallback(scaledObject *kedav1alpha1.ScaledObject, metricSpec v2.MetricSpec) bool {
	if scaledObject.Spec.Fallback == nil {
		return false
	}

	if metricSpec.External.Target.Type != v2.AverageValueMetricType {
		log.V(0).Info("Fallback can only be enabled for triggers with metric of type AverageValue",
			"scaledObject.Namespace", scaledObject.Namespace,
			"scaledObject.Name", scaledObject.Name)
		return false
	}

	return HasValidFallback(scaledObject)
}

func HasValidFallback(scaledObject *kedav1alpha1.ScaledObject) bool {
	modifierChecking := true
	if scaledObject.IsUsingModifiers() {
		value, err := strconv.ParseInt(scaledObject.Spec.Advanced.ScalingModifiers.Target, 10, 64)
		modifierChecking = err == nil && value > 0
	}
	return scaledObject.Spec.Fallback.FailureThreshold >= 0 &&
		scaledObject.Spec.Fallback.Replicas >= 0 &&
		modifierChecking
}

func hasExceededFailureThreshold(status *kedav1alpha1.ScaledObjectStatus, threshold int32) bool {
	for _, health := range status.Health {
		if health.Status == kedav1alpha1.HealthStatusFailing && *health.NumberOfFailures > threshold {
			return true
		}
	}
	return false
}

func calculateFallbackReplicas(scaledObject *kedav1alpha1.ScaledObject, currentReplicas int32) int32 {
	fallbackReplicas := scaledObject.Spec.Fallback.Replicas

	if scaledObject.Spec.Fallback.Behavior == kedav1alpha1.FallbackBehaviorUseCurrentReplicasAsMin {
		if currentReplicas > fallbackReplicas {
			return currentReplicas
		}
	}
	return fallbackReplicas
}

func calculateFallbackMetrics(scaledObject *kedav1alpha1.ScaledObject, metricSpec v2.MetricSpec, replicas int32) []external_metrics.ExternalMetricValue {
	var normalisationValue int64
	var metricName string

	if !scaledObject.IsUsingModifiers() {
		normalisationValue = int64(metricSpec.External.Target.AverageValue.AsApproximateFloat64())
		metricName = metricSpec.External.Metric.Name
	} else {
		value, _ := strconv.ParseInt(scaledObject.Spec.Advanced.ScalingModifiers.Target, 10, 64)
		normalisationValue = value
		metricName = kedav1alpha1.CompositeMetricName
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewMilliQuantity(normalisationValue*1000*int64(replicas), resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return []external_metrics.ExternalMetricValue{metric}
}

func updateStatus(ctx context.Context, client runtimeclient.Client, scaledObject *kedav1alpha1.ScaledObject,
	status *kedav1alpha1.ScaledObjectStatus, metricSpec v2.MetricSpec) {
	patch := runtimeclient.MergeFrom(scaledObject.DeepCopy())

	if hasExceededFailureThreshold(status, scaledObject.Spec.Fallback.FailureThreshold) {
		status.Conditions.SetFallbackCondition(metav1.ConditionTrue, "FallbackExists",
			"At least one trigger is falling back on this scaled object")
	} else {
		status.Conditions.SetFallbackCondition(metav1.ConditionFalse, "NoFallbackFound",
			"No fallbacks are active on this scaled object")
	}

	if !reflect.DeepEqual(scaledObject.Status, *status) {
		scaledObject.Status = *status
		err := client.Status().Patch(ctx, scaledObject, patch)
		if err != nil {
			log.Error(err, "failed to patch ScaledObjects Status",
				"scaledObject.Namespace", scaledObject.Namespace,
				"scaledObject.Name", scaledObject.Name)
		}
	}
}

func getHealthStatus(status *kedav1alpha1.ScaledObjectStatus, metricName string) *kedav1alpha1.HealthStatus {
	_, healthStatusExists := status.Health[metricName]
	if !healthStatusExists {
		zero := int32(0)
		healthStatus := kedav1alpha1.HealthStatus{
			NumberOfFailures: &zero,
			Status:          kedav1alpha1.HealthStatusHappy,
		}
		status.Health[metricName] = healthStatus
	}
	healthStatus := status.Health[metricName]
	return &healthStatus
}

func initHealthStatus(status *kedav1alpha1.ScaledObjectStatus) {
	if status.Health == nil {
		status.Health = make(map[string]kedav1alpha1.HealthStatus)
	}
}
