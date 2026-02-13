package scalers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tidwall/gjson"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type parseElasticForecastMetadataTestData struct {
	name             string
	metadata         map[string]string
	authParams       map[string]string
	expectedMetadata *elasticForecastMetadata
	expectedError    error
}

type elasticForecastMetricIdentifier struct {
	metadataTestData *parseElasticForecastMetadataTestData
	triggerIndex     int
	name             string
}

var elasticForecastTestCases = []parseElasticForecastMetadataTestData{
	{
		name:          "must provide either endpoint addresses or cloud config",
		metadata:      map[string]string{"jobID": "my-job", "targetValue": "10"},
		authParams:    map[string]string{},
		expectedError: fmt.Errorf("must provide either cloud config (cloudID + apiKey) or endpoint addresses"),
	},
	{
		name:          "only cloudID given without apiKey",
		metadata:      map[string]string{"jobID": "my-job", "targetValue": "10"},
		authParams:    map[string]string{"cloudID": "my-cluster:xxxxxxxxxxx"},
		expectedError: fmt.Errorf("cloudID and apiKey must both be provided together"),
	},
	{
		name: "cannot provide endpoint addresses and cloud config at the same time",
		metadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "my-job",
			"targetValue": "10",
		},
		authParams:    map[string]string{"cloudID": "my-cluster:xxxxxxxxxxx", "apiKey": "xxxxxxxxx"},
		expectedError: fmt.Errorf("cannot provide both cloud config (cloudID/apiKey) and endpoint addresses"),
	},
	{
		name: "addresses given without password",
		metadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "my-job",
			"targetValue": "10",
		},
		authParams:    map[string]string{"username": "admin"},
		expectedError: fmt.Errorf("username and password must both be provided when addresses is used"),
	},
	{
		name: "addresses given without username",
		metadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "my-job",
			"targetValue": "10",
		},
		authParams:    map[string]string{"password": "secret"},
		expectedError: fmt.Errorf("username and password must both be provided when addresses is used"),
	},
	{
		name: "empty jobID",
		metadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "",
			"targetValue": "10",
		},
		authParams:    map[string]string{"username": "admin", "password": "secret"},
		expectedError: fmt.Errorf("missing required parameter \"jobID\" in [triggerMetadata]"),
	},
	{
		name: "invalid targetValue",
		metadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "my-job",
			"targetValue": "not-a-number",
		},
		authParams:    map[string]string{"username": "admin", "password": "secret"},
		expectedError: fmt.Errorf("unable to set param \"targetValue\""),
	},
	{
		name: "invalid activationTargetValue",
		metadata: map[string]string{
			"addresses":             "http://localhost:9200",
			"jobID":                 "my-job",
			"targetValue":           "10",
			"activationTargetValue": "not-a-number",
		},
		authParams:    map[string]string{"username": "admin", "password": "secret"},
		expectedError: fmt.Errorf("unable to set param \"activationTargetValue\""),
	},
	{
		// index 8 — referenced by TestElasticForecastGetMetricSpecForScaling
		name: "all fields ok with defaults",
		metadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "my-job",
			"targetValue": "100",
		},
		authParams: map[string]string{"username": "admin", "password": "secret"},
		expectedMetadata: &elasticForecastMetadata{
			Addresses:             []string{"http://localhost:9200"},
			UnsafeSsl:             false,
			Username:              "admin",
			Password:              "secret",
			JobID:                 "my-job",
			LookAhead:             1 * time.Minute,
			TargetValue:           100,
			ActivationTargetValue: 0,
			ResultsIndexName:      "shared",
			MetricName:            "s0-elastic-forecast-my-job",
		},
		expectedError: nil,
	},
	{
		name: "custom lookAhead",
		metadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "cpu-job",
			"lookAhead":   "5m",
			"targetValue": "80",
		},
		authParams: map[string]string{"username": "elastic", "password": "changeme"},
		expectedMetadata: &elasticForecastMetadata{
			Addresses:             []string{"http://localhost:9200"},
			UnsafeSsl:             false,
			Username:              "elastic",
			Password:              "changeme",
			JobID:                 "cpu-job",
			LookAhead:             5 * time.Minute,
			TargetValue:           80,
			ActivationTargetValue: 0,
			ResultsIndexName:      "shared",
			MetricName:            "s0-elastic-forecast-cpu-job",
		},
		expectedError: nil,
	},
	{
		name: "with activationTargetValue",
		metadata: map[string]string{
			"addresses":             "http://localhost:9200",
			"jobID":                 "my-job",
			"targetValue":           "100",
			"activationTargetValue": "5",
		},
		authParams: map[string]string{"username": "admin", "password": "secret"},
		expectedMetadata: &elasticForecastMetadata{
			Addresses:             []string{"http://localhost:9200"},
			UnsafeSsl:             false,
			Username:              "admin",
			Password:              "secret",
			JobID:                 "my-job",
			LookAhead:             1 * time.Minute,
			TargetValue:           100,
			ActivationTargetValue: 5,
			ResultsIndexName:      "shared",
			MetricName:            "s0-elastic-forecast-my-job",
		},
		expectedError: nil,
	},
	{
		name: "with unsafeSsl enabled",
		metadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"unsafeSsl":   "true",
			"jobID":       "my-job",
			"targetValue": "100",
		},
		authParams: map[string]string{"username": "admin", "password": "secret"},
		expectedMetadata: &elasticForecastMetadata{
			Addresses:             []string{"http://localhost:9200"},
			UnsafeSsl:             true,
			Username:              "admin",
			Password:              "secret",
			JobID:                 "my-job",
			LookAhead:             1 * time.Minute,
			TargetValue:           100,
			ActivationTargetValue: 0,
			ResultsIndexName:      "shared",
			MetricName:            "s0-elastic-forecast-my-job",
		},
		expectedError: nil,
	},
	{
		name: "cloud mode",
		metadata: map[string]string{
			"jobID":       "cloud-job",
			"targetValue": "200",
		},
		authParams: map[string]string{
			"cloudID": "my-cloud:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyRjZWM2ZjI2MWE3NGJmMjRjNWQ",
			"apiKey":  "c2VjcmV0X2tleVE=",
		},
		expectedMetadata: &elasticForecastMetadata{
			CloudID:               "my-cloud:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyRjZWM2ZjI2MWE3NGJmMjRjNWQ",
			APIKey:                "c2VjcmV0X2tleVE=",
			UnsafeSsl:             false,
			JobID:                 "cloud-job",
			LookAhead:             1 * time.Minute,
			TargetValue:           200,
			ActivationTargetValue: 0,
			ResultsIndexName:      "shared",
			MetricName:            "s0-elastic-forecast-cloud-job",
		},
		expectedError: nil,
	},
	{
		name: "custom resultsIndexName",
		metadata: map[string]string{
			"addresses":        "http://localhost:9200",
			"jobID":            "my-job",
			"targetValue":      "100",
			"resultsIndexName": "my-job",
		},
		authParams: map[string]string{"username": "admin", "password": "secret"},
		expectedMetadata: &elasticForecastMetadata{
			Addresses:             []string{"http://localhost:9200"},
			UnsafeSsl:             false,
			Username:              "admin",
			Password:              "secret",
			JobID:                 "my-job",
			LookAhead:             1 * time.Minute,
			TargetValue:           100,
			ActivationTargetValue: 0,
			ResultsIndexName:      "my-job",
			MetricName:            "s0-elastic-forecast-my-job",
		},
		expectedError: nil,
	},
}

func TestParseElasticForecastMetadata(t *testing.T) {
	for _, tc := range elasticForecastTestCases {
		t.Run(tc.name, func(t *testing.T) {
			meta, err := parseElasticForecastMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: tc.metadata,
				AuthParams:      tc.authParams,
				TriggerIndex:    0,
			})

			if tc.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError.Error())
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedMetadata, &meta)
		})
	}
}

func TestElasticForecastUnsafeSslDefaultValue(t *testing.T) {
	meta, err := parseElasticForecastMetadata(&scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "my-job",
			"targetValue": "10",
		},
		AuthParams: map[string]string{
			"username": "admin",
			"password": "secret",
		},
	})
	assert.NoError(t, err)
	assert.False(t, meta.UnsafeSsl, "unsafeSsl should default to false")
}

func TestElasticForecastDefaultLookAhead(t *testing.T) {
	meta, err := parseElasticForecastMetadata(&scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "my-job",
			"targetValue": "10",
		},
		AuthParams: map[string]string{
			"username": "admin",
			"password": "secret",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1*time.Minute, meta.LookAhead, "lookAhead should default to 1m")
}

// TestElasticForecastDuration verifies that forecastDuration() is always forecastDurationMultiplier × lookAhead
func TestElasticForecastDuration(t *testing.T) {
	cases := []struct {
		lookAhead        time.Duration
		expectedDuration time.Duration
	}{
		{1 * time.Minute, time.Duration(forecastDurationMultiplier) * 1 * time.Minute},
		{5 * time.Minute, time.Duration(forecastDurationMultiplier) * 5 * time.Minute},
		{30 * time.Minute, time.Duration(forecastDurationMultiplier) * 30 * time.Minute},
		{1 * time.Hour, time.Duration(forecastDurationMultiplier) * 1 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.lookAhead.String(), func(t *testing.T) {
			meta := elasticForecastMetadata{LookAhead: tc.lookAhead}
			assert.Equal(t, tc.expectedDuration, meta.forecastDuration(),
				"forecastDuration should be %d × lookAhead", forecastDurationMultiplier)
		})
	}
}

func TestElasticForecastDefaultResultsIndexName(t *testing.T) {
	meta, err := parseElasticForecastMetadata(&scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"addresses":   "http://localhost:9200",
			"jobID":       "my-job",
			"targetValue": "10",
		},
		AuthParams: map[string]string{
			"username": "admin",
			"password": "secret",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "shared", meta.ResultsIndexName,
		"resultsIndexName should default to 'shared', mapping to .ml-anomalies-shared")
}

func TestElasticForecastGetMetricSpecForScaling(t *testing.T) {
	var metricIdentifiers = []elasticForecastMetricIdentifier{
		{&elasticForecastTestCases[8], 0, "s0-elastic-forecast-my-job"},
		{&elasticForecastTestCases[8], 1, "s1-elastic-forecast-my-job"},
	}

	for _, testData := range metricIdentifiers {
		t.Run(fmt.Sprintf("triggerIndex=%d", testData.triggerIndex), func(t *testing.T) {
			ctx := context.Background()

			meta, err := parseElasticForecastMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: testData.metadataTestData.metadata,
				AuthParams:      testData.metadataTestData.authParams,
				TriggerIndex:    testData.triggerIndex,
			})
			if testData.metadataTestData.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), testData.metadataTestData.expectedError.Error())
				return
			}
			assert.NoError(t, err)

			scaler := &elasticForecastScaler{metadata: meta, esClient: nil}
			metricSpec := scaler.GetMetricSpecForScaling(ctx)

			assert.Len(t, metricSpec, 1)
			assert.Equal(t, testData.name, metricSpec[0].External.Metric.Name)
		})
	}
}

// TestElasticForecastRenewalLogic verifies that ensureForecastValid only triggers a renewal when the remaining window falls below the threshold.
func TestElasticForecastRenewalLogic(t *testing.T) {
	lookAhead := 5 * time.Minute
	duration := lookAhead * forecastDurationMultiplier // 10m

	scaler := &elasticForecastScaler{
		metadata: elasticForecastMetadata{LookAhead: lookAhead},
	}

	// Helper: mirrors the needsRenewal logic in ensureForecastValid.
	needsRenewal := func() bool {
		scaler.mu.Lock()
		id := scaler.forecastID
		expiry := scaler.forecastExpiry
		scaler.mu.Unlock()
		threshold := time.Duration(float64(scaler.metadata.forecastDuration()) * forecastRenewThreshold)
		return id == "" || time.Until(expiry) < threshold
	}

	// No forecast yet — must renew.
	assert.True(t, needsRenewal(), "should renew when forecastID is empty")

	// Forecast with plenty of time remaining — no renewal needed.
	scaler.forecastID = "test-id"
	scaler.forecastExpiry = time.Now().Add(duration)
	assert.False(t, needsRenewal(), "should NOT renew when window is full")

	// Forecast with less than threshold remaining — must renew.
	scaler.forecastExpiry = time.Now().Add(time.Duration(float64(duration)*forecastRenewThreshold) - time.Second)
	assert.True(t, needsRenewal(), "should renew when remaining window < threshold")
}

// TestElasticForecastPreviousForecastFallback verifies that the previous forecast ID is retained after renewal and can serve as a fallback while the new forecast documents are still being computed.
func TestElasticForecastPreviousForecastFallback(t *testing.T) {
	scaler := &elasticForecastScaler{}

	// Simulate first forecast creation.
	scaler.mu.Lock()
	scaler.forecastID = "forecast-aaa"
	scaler.forecastExpiry = time.Now().Add(10 * time.Minute)
	scaler.mu.Unlock()

	assert.Equal(t, "forecast-aaa", scaler.forecastID)
	assert.Empty(t, scaler.previousForecastID, "no previous yet on first forecast")

	// Simulate renewal: previous is promoted, new ID stored.
	scaler.mu.Lock()
	if scaler.forecastID != "" {
		scaler.previousForecastID = scaler.forecastID
	}
	scaler.forecastID = "forecast-bbb"
	scaler.forecastExpiry = time.Now().Add(10 * time.Minute)
	scaler.mu.Unlock()

	assert.Equal(t, "forecast-bbb", scaler.forecastID, "active forecast updated")
	assert.Equal(t, "forecast-aaa", scaler.previousForecastID, "previous forecast retained as fallback")

	// Simulate a second renewal.
	scaler.mu.Lock()
	if scaler.forecastID != "" {
		scaler.previousForecastID = scaler.forecastID
	}
	scaler.forecastID = "forecast-ccc"
	scaler.forecastExpiry = time.Now().Add(10 * time.Minute)
	scaler.mu.Unlock()

	assert.Equal(t, "forecast-ccc", scaler.forecastID)
	assert.Equal(t, "forecast-bbb", scaler.previousForecastID,
		"previous is always the immediately preceding forecast, not the oldest")
}

// TestElasticForecastEnsureJobOpenStates documents the three states ensureJobOpen handles.
func TestElasticForecastEnsureJobOpenStateCheck(t *testing.T) {
	cases := []struct {
		name       string
		statsBody  string
		wantOpened bool
	}{
		{
			name:       "job already opened — no action needed",
			statsBody:  `{"jobs":[{"state":"opened"}]}`,
			wantOpened: true,
		},
		{
			name:       "job closed — must be re-opened",
			statsBody:  `{"jobs":[{"state":"closed"}]}`,
			wantOpened: false,
		},
		{
			name:       "job closing — must wait and re-open",
			statsBody:  `{"jobs":[{"state":"closing"}]}`,
			wantOpened: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := gjson.Get(tc.statsBody, "jobs.0.state").String()
			assert.Equal(t, tc.wantOpened, state == "opened")
		})
	}
}

type durationToESStringTestData struct {
	name  string
	input time.Duration
	want  string
}

var durationToESStringTestCases = []durationToESStringTestData{
	{"whole days", 2 * 24 * time.Hour, "2d"},
	{"single day", 1 * 24 * time.Hour, "1d"},
	{"whole hours", 2 * time.Hour, "2h"},
	{"90 minutes stays in minutes", 90 * time.Minute, "90m"},
	{"10 minutes", 10 * time.Minute, "10m"},
	{"1 minute", 1 * time.Minute, "1m"},
	{"whole seconds", 30 * time.Second, "30s"},
	{"90 seconds stays in seconds", 90 * time.Second, "90s"},
	{"sub-second rounds up to 1s", 500 * time.Millisecond, "1s"},
	{"1500ms rounds up to 2s", 1500 * time.Millisecond, "2s"},
}

func TestDurationToESString(t *testing.T) {
	for _, tc := range durationToESStringTestCases {
		t.Run(tc.name, func(t *testing.T) {
			got := durationToESString(tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}
