package scalers

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	v2 "k8s.io/api/autoscaling/v2"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type parseNewRelicMetadataTestData struct {
	metadata   map[string]string
	authParams map[string]string
	isError    bool
}

type newrelicMetricIdentifier struct {
	metadataTestData *parseNewRelicMetadataTestData
	triggerIndex     int
	name             string
}

var testNewRelicMetadata = []parseNewRelicMetadataTestData{
	{map[string]string{}, map[string]string{}, true},
	// all properly formed
	{map[string]string{"account": "0", "threshold": "100", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, false},
	// all properly formed with region and activationThreshold
	{map[string]string{"account": "0", "region": "EU", "threshold": "100", "activationThreshold": "20", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, false},
	// account passed via auth params
	{map[string]string{"region": "EU", "threshold": "100", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{"account": "0"}, false},
	// region passed via auth params
	{map[string]string{"account": "0", "threshold": "100", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{"region": "EU"}, false},
	// account as String
	{map[string]string{"account": "ABC", "threshold": "100", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, true},
	// missing account
	{map[string]string{"threshold": "100", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, true},
	// missing account
	{map[string]string{"threshold": "100", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, true},
	// malformed threshold
	{map[string]string{"account": "0", "threshold": "one", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, true},
	// malformed activationThreshold
	{map[string]string{"account": "0", "threshold": "100", "activationThreshold": "notanint", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, true},
	// missing threshold
	{map[string]string{"account": "0", "queryKey": "somekey", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, true},
	// missing query
	{map[string]string{"account": "0", "threshold": "100", "queryKey": "somekey"}, map[string]string{}, true},
	// noDataError invalid value
	{map[string]string{"account": "0", "threshold": "100", "queryKey": "somekey", "noDataError": "invalid", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, true},
	// noDataError valid values
	{map[string]string{"account": "0", "threshold": "100", "queryKey": "somekey", "noDataError": "true", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, false},
	{map[string]string{"account": "0", "threshold": "100", "queryKey": "somekey", "noDataError": "false", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, false},
	{map[string]string{"account": "0", "threshold": "100", "queryKey": "somekey", "noDataError": "0", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, false},
	{map[string]string{"account": "0", "threshold": "100", "queryKey": "somekey", "noDataError": "1", "nrql": "SELECT average(cpuUsedCores) as result FROM K8sContainerSample WHERE containerName='coredns'"}, map[string]string{}, false},
	// percentile query with float threshold
	{map[string]string{"account": "0", "threshold": "25.5", "queryKey": "somekey", "nrql": "SELECT percentile(jobs_graphql_event_loop_lag, 99.5) FROM Metric WHERE service.name='jobs-graphql-staging' SINCE 150 seconds ago"}, map[string]string{}, false},
	// percentile query with activationThreshold
	{map[string]string{"account": "0", "threshold": "30.0", "activationThreshold": "10.5", "queryKey": "somekey", "nrql": "SELECT percentile(duration, 95) FROM Transaction WHERE appName='MyApp'"}, map[string]string{}, false},
}

var newrelicMetricIdentifiers = []newrelicMetricIdentifier{
	{&testNewRelicMetadata[1], 0, "s0-new-relic"},
	{&testNewRelicMetadata[1], 1, "s1-new-relic"},
	{&testNewRelicMetadata[17], 0, "s0-new-relic"},
	{&testNewRelicMetadata[18], 2, "s2-new-relic"},
}

func TestNewRelicParseMetadata(t *testing.T) {
	for i, testData := range testNewRelicMetadata {
		t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
			_, err := parseNewRelicMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: testData.metadata,
				AuthParams:      testData.authParams,
			})
			if err != nil && !testData.isError {
				t.Errorf("Test case %d: Expected success but got error: %v\nMetadata: %v\nAuthParams: %v",
					i, err, testData.metadata, testData.authParams)
			}
			if testData.isError && err == nil {
				t.Errorf("Test case %d: Expected error but got success\nMetadata: %v\nAuthParams: %v",
					i, testData.metadata, testData.authParams)
			}
		})
	}
}

func TestNewRelicGetMetricSpecForScaling(t *testing.T) {
	for _, testData := range newrelicMetricIdentifiers {
		meta, err := parseNewRelicMetadata(&scalersconfig.ScalerConfig{
			TriggerMetadata: testData.metadataTestData.metadata,
			AuthParams:      testData.metadataTestData.authParams,
			TriggerIndex:    testData.triggerIndex,
		})
		if err != nil {
			t.Fatal("Could not parse metadata:", err)
		}
		mockNewRelicScaler := newrelicScaler{
			metadata:   meta,
			nrClient:   nil,
			logger:     logr.Discard(),
			metricType: v2.AverageValueMetricType,
		}

		metricSpec := mockNewRelicScaler.GetMetricSpecForScaling(context.Background())
		metricName := metricSpec[0].External.Metric.Name
		if metricName != testData.name {
			t.Error("Wrong External metric source name:", metricName)
		}
	}
}

func TestToFloat64(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected *float64
		isNil    bool
	}{
		{"float64 input", 24.625, floatPtr(24.625), false},
		{"string float input", "24.625", floatPtr(24.625), false},
		{"string int input", "42", floatPtr(42.0), false},
		{"invalid string", "not-a-number", nil, true},
		{"nil input", nil, nil, true},
		{"int input", 42, nil, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := toFloat64(tc.input)
			if tc.isNil {
				if result != nil {
					t.Errorf("Expected nil, got %v", *result)
				}
			} else {
				if result == nil {
					t.Errorf("Expected %f, got nil", *tc.expected)
				} else if *result != *tc.expected {
					t.Errorf("Expected %f, got %f", *tc.expected, *result)
				}
			}
		})
	}
}

// Helper function
func floatPtr(f float64) *float64 {
	return &f
}
