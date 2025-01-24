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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/go-logr/logr"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/azure"
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

// MockBlobResponse simulate Azure Blob response
type MockBlobResponse struct {
	Segment struct {
		BlobItems []azblob.BlobItem
	}
}

// MockPager simulate Azure Blob paging
type MockPager struct {
	items    []string
	position int
}

func (p *MockPager) More() bool {
	return p.position < len(p.items)
}

func (p *MockPager) NextPage(ctx context.Context) (*MockBlobResponse, error) {
	if !p.More() {
		return nil, nil
	}

	response := &MockBlobResponse{}
	response.Segment.BlobItems = make([]azblob.BlobItem, 1)
	
	name := p.items[p.position]
	blobItem := azblob.BlobItem{
		Name: &name,
	}
	response.Segment.BlobItems[0] = blobItem
	
	p.position++
	return response, nil
}

// MockAzureBlobClient simulate Azure Blob client
type MockAzureBlobClient struct {
	blobItems []string
}

func (m *MockAzureBlobClient) NewListBlobsFlatPager(options *azblob.ListBlobsFlatOptions) azblob.ListBlobsFlatPager {
	return &MockPager{
		items:    m.blobItems,
		position: 0,
	}
}

func TestAzureBlobScalerWithGlobPattern(t *testing.T) {
	testCases := []struct {
		name          string
		globPattern   string
		blobItems     []string
		expectedCount int64
	}{
		{
			name:        "JSON files in specific folder",
			globPattern: "folderA/subFolderA/*.json",
			blobItems: []string{
				"folderA/subFolderA/part-fileA.json",
				"folderA/subFolderA/part-fileB.json",
				"folderA/subFolderA/_METADATA_FILE",
				"folderA/subFolderB/fileD.json",
				"folderB/subFolderC/fileE.txt",
			},
			expectedCount: 2,
		},
		{
			name:        "No matching files",
			globPattern: "folderA/subFolderA/*.csv",
			blobItems: []string{
				"folderA/subFolderA/part-fileA.json",
				"folderA/subFolderA/part-fileB.json",
			},
			expectedCount: 0,
		},
		{
			name:        "All JSON files recursively",
			globPattern: "**/*.json",
			blobItems: []string{
				"folderA/subFolderA/part-fileA.json",
				"folderA/subFolderA/part-fileB.json",
				"folderA/subFolderB/fileD.json",
				"folderB/subFolderC/fileE.txt",
			},
			expectedCount: 3,
		},
		{
			name:        "Specific prefix JSON files",
			globPattern: "folderA/subFolderA/part-*.json",
			blobItems: []string{
				"folderA/subFolderA/part-fileA.json",
				"folderA/subFolderA/part-fileB.json",
				"folderA/subFolderA/other.json",
				"folderA/subFolderA/_METADATA_FILE",
			},
			expectedCount: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := map[string]string{
				"blobContainerName":    "container",
				"activationBlobCount":  "1",
				"blobCount":           "5",
				"connectionFromEnv":    "CONNECTION",
				"accountName":         "adlsaccount",
				"globPattern":         tc.globPattern,
			}

			resolvedEnv := map[string]string{
				"CONNECTION": "SAMPLE",
			}

			meta, _, err := parseAzureBlobMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: metadata,
				ResolvedEnv:     resolvedEnv,
				AuthParams:      map[string]string{},
			}, logr.Discard())

			if err != nil {
				t.Fatalf("Error parsing metadata: %v", err)
			}

			mockClient := &MockAzureBlobClient{
				blobItems: tc.blobItems,
			}

			count, err := azure.GetAzureBlobListLength(
				context.Background(),
				mockClient,
				meta,
			)

			if err != nil {
				t.Fatalf("Error getting blob list length: %v", err)
			}

			if count != tc.expectedCount {
				t.Errorf("Expected count: %d, got: %d", tc.expectedCount, count)
				t.Logf("Glob pattern: %s", tc.globPattern)
				t.Logf("Available files:")
				for _, item := range tc.blobItems {
					t.Logf("- %s", item)
				}
			}
		})
	}
}
