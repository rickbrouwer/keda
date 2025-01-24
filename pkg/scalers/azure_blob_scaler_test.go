/*
Copyright 2021 The KEDA Authors

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

package scalers

import (
	"context"
	"testing"

	"github.com/gobwas/glob"
	"github.com/go-logr/logr"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

var testAzBlobResolvedEnv = map[string]string{
	"CONNECTION": "SAMPLE",
}

type parseAzBlobMetadataTestData struct {
	metadata    map[string]string
	isError     bool
	resolvedEnv map[string]string
	authParams  map[string]string
	podIdentity kedav1alpha1.PodIdentityProvider
}

type azBlobMetricIdentifier struct {
	metadataTestData *parseAzBlobMetadataTestData
	triggerIndex     int
	name             string
}

var testAzBlobMetadata = []parseAzBlobMetadataTestData{
	// nothing passed
	{map[string]string{}, true, testAzBlobResolvedEnv, map[string]string{}, ""},
	// properly formed
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "5", "blobDelimiter": "/", "blobPrefix": "blobsubpath"}, false, testAzBlobResolvedEnv, map[string]string{}, ""},
	// Empty blobcontainerName
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": ""}, true, testAzBlobResolvedEnv, map[string]string{}, ""},
	// improperly formed blobCount
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "AA"}, true, testAzBlobResolvedEnv, map[string]string{}, ""},
	// improperly formed activationBlobCount
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "1", "activationBlobCount": "AA"}, true, testAzBlobResolvedEnv, map[string]string{}, ""},
	// podIdentity = azure-workload with account name
	{map[string]string{"accountName": "sample_acc", "blobContainerName": "sample_container"}, false, testAzBlobResolvedEnv, map[string]string{}, kedav1alpha1.PodIdentityProviderAzureWorkload},
	// podIdentity = azure-workload without account name
	{map[string]string{"accountName": "", "blobContainerName": "sample_container"}, true, testAzBlobResolvedEnv, map[string]string{}, kedav1alpha1.PodIdentityProviderAzureWorkload},
	// podIdentity = azure-workload without blob container name
	{map[string]string{"accountName": "sample_acc", "blobContainerName": ""}, true, testAzBlobResolvedEnv, map[string]string{}, kedav1alpha1.PodIdentityProviderAzureWorkload},
	// podIdentity = azure-workload with cloud
	{map[string]string{"accountName": "sample_acc", "blobContainerName": "sample_container", "cloud": "AzureGermanCloud"}, false, testAzBlobResolvedEnv, map[string]string{}, kedav1alpha1.PodIdentityProviderAzureWorkload},
	// podIdentity = azure-workload with invalid cloud
	{map[string]string{"accountName": "sample_acc", "blobContainerName": "sample_container", "cloud": "InvalidCloud"}, true, testAzBlobResolvedEnv, map[string]string{}, kedav1alpha1.PodIdentityProviderAzureWorkload},
	// podIdentity = azure-workload with private cloud and endpoint suffix
	{map[string]string{"accountName": "sample_acc", "blobContainerName": "sample_container", "cloud": "Private", "endpointSuffix": "queue.core.private.cloud"}, false, testAzBlobResolvedEnv, map[string]string{}, kedav1alpha1.PodIdentityProviderAzureWorkload},
	// podIdentity = azure-workload with private cloud and no endpoint suffix
	{map[string]string{"accountName": "sample_acc", "blobContainerName": "sample_container", "cloud": "Private", "endpointSuffix": ""}, true, testAzBlobResolvedEnv, map[string]string{}, kedav1alpha1.PodIdentityProviderAzureWorkload},
	// podIdentity = azure-workload with endpoint suffix and no cloud
	{map[string]string{"accountName": "sample_acc", "blobContainerName": "sample_container", "cloud": "", "endpointSuffix": "ignored"}, false, testAzBlobResolvedEnv, map[string]string{}, kedav1alpha1.PodIdentityProviderAzureWorkload},
	// connection from authParams
	{map[string]string{"blobContainerName": "sample_container", "blobCount": "5"}, false, testAzBlobResolvedEnv, map[string]string{"connection": "value"}, kedav1alpha1.PodIdentityProviderNone},
	// with globPattern
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "5", "globPattern": "foo**"}, false, testAzBlobResolvedEnv, map[string]string{}, ""},
	// with recursive true
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "5", "recursive": "true"}, false, testAzBlobResolvedEnv, map[string]string{}, ""},
	// with recursive false
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "5", "recursive": "false"}, false, testAzBlobResolvedEnv, map[string]string{}, ""},
	// with invalid value for recursive
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "5", "recursive": "invalid"}, true, testAzBlobResolvedEnv, map[string]string{}, ""},
	// with invalid glob pattern
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "5", "globPattern": "[\\]"}, true, testAzBlobResolvedEnv, map[string]string{}, ""},
	// test with path specific glob pattern
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "5", "globPattern": "folderA/subFolderA/*.json"}, false, testAzBlobResolvedEnv, map[string]string{}, ""},
	// test with double asteriks glob pattern
	{map[string]string{"connectionFromEnv": "CONNECTION", "blobContainerName": "sample", "blobCount": "5", "globPattern": "**/subFolderA/*.json"}, false, testAzBlobResolvedEnv, map[string]string{}, ""},
}

var azBlobMetricIdentifiers = []azBlobMetricIdentifier{
	{&testAzBlobMetadata[1], 0, "s0-azure-blob-sample"},
	{&testAzBlobMetadata[5], 1, "s1-azure-blob-sample_container"},
}

func TestAzBlobParseMetadata(t *testing.T) {
	for _, testData := range testAzBlobMetadata {
		_, podIdentity, err := parseAzureBlobMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata, ResolvedEnv: testData.resolvedEnv,
			AuthParams: testData.authParams, PodIdentity: kedav1alpha1.AuthPodIdentity{Provider: testData.podIdentity}}, logr.Discard())
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Errorf("Expected error but got success. testData: %v", testData)
		}
		if testData.podIdentity != "" && testData.podIdentity != podIdentity.Provider && err == nil {
			t.Error("Expected success but got error: podIdentity value is not returned as expected")
		}
	}
}

func TestAzBlobGetMetricSpecForScaling(t *testing.T) {
	for _, testData := range azBlobMetricIdentifiers {
		ctx := context.Background()
		meta, podIdentity, err := parseAzureBlobMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadataTestData.metadata,
			ResolvedEnv: testData.metadataTestData.resolvedEnv, AuthParams: testData.metadataTestData.authParams,
			PodIdentity: kedav1alpha1.AuthPodIdentity{Provider: testData.metadataTestData.podIdentity}, TriggerIndex: testData.triggerIndex}, logr.Discard())
		if err != nil {
			t.Fatal("Could not parse metadata:", err)
		}
		mockAzBlobScaler := azureBlobScaler{
			metadata:    meta,
			podIdentity: podIdentity,
		}

		metricSpec := mockAzBlobScaler.GetMetricSpecForScaling(ctx)
		metricName := metricSpec[0].External.Metric.Name
		if metricName != testData.name {
			t.Error("Wrong External metric source name:", metricName)
		}
	}
}

func TestAzureBlobGlobPatternMatching(t *testing.T) {
	testCases := []struct {
		name       string
		pattern    string
		blobPaths  []string
		shouldMatch []bool
	}{
		{
			name:    "Simple JSON pattern",
			pattern: "folderA/subFolderA/*.json",
			blobPaths: []string{
				"folderA/subFolderA/file1.json",
				"folderA/subFolderA/file2.json",
				"folderA/subFolderB/file3.json",
				"folderA/subFolderA/file4.txt",
			},
			shouldMatch: []bool{true, true, false, false},
		},
		{
			name:    "Specific prefix pattern",
			pattern: "folderA/subFolderA/part-*.json",
			blobPaths: []string{
				"folderA/subFolderA/part-fileA.json",
				"folderA/subFolderA/part-fileB.json",
				"folderA/subFolderA/_METADATA_FILE",
				"folderA/subFolderA/other.json",
			},
			shouldMatch: []bool{true, true, false, false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			glob, err := glob.Compile(tc.pattern)
			if err != nil {
				t.Fatalf("Failed to compile glob pattern: %v", err)
			}

			for i, path := range tc.blobPaths {
				matches := glob.Match(path)
				if matches != tc.shouldMatch[i] {
					t.Errorf("Path '%s' with pattern '%s': expected match=%v, got match=%v",
					path, tc.pattern, tc.shouldMatch[i], matches)
				}
			}
		})
	}
}

func TestAzureBlobGlobPatternCount(t *testing.T) {
	testCases := []struct {
		name         string
		pattern      string
		blobPaths    []string
		expectedCount int64
	}{
		{
			name:    "Count JSON files in folderA/subFolderA",
			pattern: "folderA/subFolderA/*.json",
			blobPaths: []string{
				"folderA/subFolderA/part-fileA.json",
				"folderA/subFolderA/part-fileB.json",
				"folderA/subFolderA/_METADATA_FILE",
				"folderA/subFolderB/fileD.json",
				"folderB/subFolderC/fileE.txt",
			},
			expectedCount: 2,
		},
		{
			name:    "Count all JSON files",
			pattern: "**/*.json",
			blobPaths: []string{
				"folderA/subFolderA/part-fileA.json",
				"folderA/subFolderA/part-fileB.json",
				"folderA/subFolderA/_METADATA_FILE",
				"folderA/subFolderB/fileD.json",
				"folderB/subFolderC/fileE.txt",
			},
			expectedCount: 3,
		},
		{
			name:    "Count part- prefixed JSON files",
			pattern: "folderA/subFolderA/part-*.json",
			blobPaths: []string{
				"folderA/subFolderA/part-fileA.json",
				"folderA/subFolderA/part-fileB.json",
				"folderA/subFolderA/_METADATA_FILE",
				"folderA/subFolderB/fileD.json",
				"folderB/subFolderC/fileE.txt",
			},
			expectedCount: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			glob, err := glob.Compile(tc.pattern)
			if err != nil {
				t.Fatalf("Failed to compile glob pattern: %v", err)
			}
	
			// Count matches
			var count int64
			for _, path := range tc.blobPaths {
				if glob.Match(path) {
					count++
				}
			}
	
			if count != tc.expectedCount {
				t.Errorf("Pattern '%s' matched %d files, expected %d matches.\nMatched files:", 
				tc.pattern, count, tc.expectedCount)
				// Print matching files for debug
				for _, path := range tc.blobPaths {
					if glob.Match(path) {
						t.Logf("- %s", path)
					}
				}
			}
		})
	}
}
