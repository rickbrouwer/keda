package scalers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type parseCronMetadataTestData struct {
	metadata map[string]string
	isError  bool
}

type cronMetricIdentifier struct {
	metadataTestData *parseCronMetadataTestData
	triggerIndex     int
	name             string
}

// A complete valid metadata example for reference
var validCronMetadata = map[string]string{
	"timezone":        "Etc/UTC",
	"start":           "0 0 * * Thu",
	"end":             "59 23 * * Thu",
	"desiredReplicas": "10",
}

// A complete valid metadata example which is enabled every even hours
var validCronMetadata2 = map[string]string{
	"timezone":        "Etc/UTC",
	"start":           "0 */2 * * *",    // Every 2 hours (even)
	"end":             "0 1-23/2 * * *", // Every 2 hours starting at 1 (odd)
	"desiredReplicas": "10",
}

// A valid metadata example without desiredReplicas (should use minReplicas/maxReplicas)
var validCronMetadataNoDesiredReplicas = map[string]string{
	"timezone": "Etc/UTC",
	"start":    "0 0 * * Thu",
	"end":      "59 23 * * Thu",
}

// A valid metadata example for testing the range use case without desiredReplicas
var validCronMetadataNoDesiredReplicas2 = map[string]string{
	"timezone": "Etc/UTC",
	"start":    "0 */2 * * *",    // Every 2 hours (even)
	"end":      "0 1-23/2 * * *", // Every 2 hours starting at 1 (odd)
}

var testCronMetadata = []parseCronMetadataTestData{
	{map[string]string{}, true},
	{validCronMetadata, false},
	{validCronMetadata2, false},
	{validCronMetadataNoDesiredReplicas, false},
	{validCronMetadataNoDesiredReplicas2, false},
	{map[string]string{"timezone": "Asia/Kolkata", "start": "30 * * * *", "end": "45 * * * *"}, true},
	{map[string]string{"start": "30 * * * *", "end": "45 * * * *"}, true},
	{map[string]string{"timezone": "Asia/Kolkata", "start": "30-33 * * * *", "end": "45 * * * *", "desiredReplicas": "10"}, false},
	{map[string]string{"timezone": "Asia/Kolkata", "start": "30 * * * *", "end": "45-50 * * * *", "desiredReplicas": "10"}, false},
	{map[string]string{"timezone": "Asia/Kolkata", "start": "-30 * * * *", "end": "45 * * * *", "desiredReplicas": "10"}, true},
	{map[string]string{"timezone": "Asia/Kolkata", "start": "30 * * * *", "end": "-50 * * * *", "desiredReplicas": "10"}, true},
	{map[string]string{"timezone": "Asia/Kolkata", "start": "30 * * * *", "end": "50 * * -3 *", "desiredReplicas": "10"}, true},
	{map[string]string{"timezone": "Asia/Kolkata", "start": "30 * * * *", "end": "30 * * * *", "desiredReplicas": "10"}, true},
}

var cronMetricIdentifiers = []cronMetricIdentifier{
	{&testCronMetadata[1], 0, "s0-cron-Etc-UTC-00xxThu-5923xxThu"},
	{&testCronMetadata[2], 1, "s1-cron-Etc-UTC-0xSl2xxx-01-23Sl2xxx"},
	{&testCronMetadata[3], 2, "s2-cron-Etc-UTC-00xxThu-5923xxThu"},
}

var tz, _ = time.LoadLocation(validCronMetadata2["timezone"])
var currentDay = time.Now().In(tz).Weekday().String()
var currentHour = time.Now().In(tz).Hour()

func TestCronParseMetadata(t *testing.T) {
	for _, testData := range testCronMetadata {
		_, err := parseCronMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata})
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}

func TestIsActive(t *testing.T) {
	scaler, _ := NewCronScaler(&scalersconfig.ScalerConfig{TriggerMetadata: validCronMetadata})
	_, isActive, _ := scaler.GetMetricsAndActivity(context.TODO(), "ReplicaCount")
	if currentDay == "Thursday" {
		assert.Equal(t, isActive, true)
	} else {
		assert.Equal(t, isActive, false)
	}
}

func TestIsActiveRange(t *testing.T) {
	scaler, _ := NewCronScaler(&scalersconfig.ScalerConfig{TriggerMetadata: validCronMetadata2})
	_, isActive, _ := scaler.GetMetricsAndActivity(context.TODO(), "ReplicaCount")
	if currentHour%2 == 0 {
		assert.Equal(t, isActive, true)
	} else {
		assert.Equal(t, isActive, false)
	}
}

func TestGetMetrics(t *testing.T) {
	scaler, _ := NewCronScaler(&scalersconfig.ScalerConfig{TriggerMetadata: validCronMetadata})
	metrics, _, _ := scaler.GetMetricsAndActivity(context.TODO(), "ReplicaCount")
	assert.Equal(t, metrics[0].MetricName, "ReplicaCount")
	if currentDay == "Thursday" {
		assert.Equal(t, metrics[0].Value.Value(), int64(10))
	} else {
		assert.Equal(t, metrics[0].Value.Value(), int64(1))
	}
}

func TestGetMetricsRange(t *testing.T) {
	scaler, _ := NewCronScaler(&scalersconfig.ScalerConfig{TriggerMetadata: validCronMetadata2})
	metrics, _, _ := scaler.GetMetricsAndActivity(context.TODO(), "ReplicaCount")
	assert.Equal(t, metrics[0].MetricName, "ReplicaCount")
	if currentHour%2 == 0 {
		assert.Equal(t, metrics[0].Value.Value(), int64(10))
	} else {
		assert.Equal(t, metrics[0].Value.Value(), int64(1))
	}
}

// Test metrics when desiredReplicas is not provided
func TestGetMetricsWithoutDesiredReplicas(t *testing.T) {
	// Create scaler with min=2 and max=8
	scaler, _ := NewCronScaler(&scalersconfig.ScalerConfig{
		TriggerMetadata: validCronMetadataNoDesiredReplicas,
		MinReplicas:     2,
		MaxReplicas:     8,
	})
	
	metrics, _, _ := scaler.GetMetricsAndActivity(context.TODO(), "ReplicaCount")
	assert.Equal(t, metrics[0].MetricName, "ReplicaCount")
	
	if currentDay == "Thursday" {
		// Should use maxReplicas inside the interval
		assert.Equal(t, metrics[0].Value.Value(), int64(8))
	} else {
		// Should use minReplicas outside the interval
		assert.Equal(t, metrics[0].Value.Value(), int64(2))
	}
}

// Test metrics when desiredReplicas is not provided for range pattern
func TestGetMetricsRangeWithoutDesiredReplicas(t *testing.T) {
	// Create scaler with min=3 and max=9
	scaler, _ := NewCronScaler(&scalersconfig.ScalerConfig{
		TriggerMetadata: validCronMetadataNoDesiredReplicas2,
		MinReplicas:     3,
		MaxReplicas:     9,
	})
	
	metrics, _, _ := scaler.GetMetricsAndActivity(context.TODO(), "ReplicaCount")
	assert.Equal(t, metrics[0].MetricName, "ReplicaCount")
	
	if currentHour%2 == 0 {
		// Should use maxReplicas inside the interval
		assert.Equal(t, metrics[0].Value.Value(), int64(9))
	} else {
		// Should use minReplicas outside the interval
		assert.Equal(t, metrics[0].Value.Value(), int64(3))
	}
}

// Test behavior with mixed configuration - desiredReplicas takes precedence when provided
func TestDesiredReplicasPrecedence(t *testing.T) {
	// Create scaler with min=2 and max=8 but also with desiredReplicas=5
	scalerConfig := &scalersconfig.ScalerConfig{
		TriggerMetadata: validCronMetadata,  // This has desiredReplicas: 10
		MinReplicas:     2,
		MaxReplicas:     8,
	}
	
	scaler, _ := NewCronScaler(scalerConfig)
	metrics, _, _ := scaler.GetMetricsAndActivity(context.Background(), "ReplicaCount")
	
	if currentDay == "Thursday" {
		// Should use desiredReplicas (10) even though max is 8 
		assert.Equal(t, metrics[0].Value.Value(), int64(10))
	} else {
		// Should use minReplicas outside the interval
		assert.Equal(t, metrics[0].Value.Value(), int64(2))
	}
}

func TestCronGetMetricSpecForScaling(t *testing.T) {
	for _, testData := range cronMetricIdentifiers {
		meta, err := parseCronMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadataTestData.metadata, TriggerIndex: testData.triggerIndex})
		if err != nil {
			t.Fatal("Could not parse metadata:", err)
		}

		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		startSchedule, _ := parser.Parse(meta.Start)
		endSchedule, _ := parser.Parse(meta.End)

		mockCronScaler := cronScaler{
			metricType:    "",
			metadata:      meta,
			logger:        logr.Discard(),
			startSchedule: startSchedule,
			endSchedule:   endSchedule,
		}

		metricSpec := mockCronScaler.GetMetricSpecForScaling(context.Background())
		metricName := metricSpec[0].External.Metric.Name
		if metricName != testData.name {
			t.Error("Wrong External metric source name:", metricName)
		}
	}
}
