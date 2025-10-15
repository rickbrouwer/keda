/*
Copyright 2024 The KEDA Authors

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

/*
This file contains the logic for parsing trigger information into
a common AuthorizationMetadata. This also contains the logic for
getting *aws.Config from a given AuthorizationMetadata, recovering
from the cache if it's a method which supports caching.
*/

package aws

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
)

// ErrAwsNoAccessKey is returned when awsAccessKeyID is missing.
var ErrAwsNoAccessKey = errors.New("awsAccessKeyID not found")

var awsSharedCredentialsCache = newSharedConfigsCache()

type AwsAuthConfig struct {
	AwsRegion          string `keda:"name=awsRegion,           order=triggerMetadata"`
	AwsAccessKeyID     string `keda:"name=awsAccessKeyID,      order=triggerMetadata;authParams;resolvedEnv, optional"`
	AwsSecretAccessKey string `keda:"name=awsSecretAccessKey,  order=authParams;resolvedEnv, optional"`
	AwsSessionToken    string `keda:"name=awsSessionToken,     order=authParams, optional"`
	AwsRoleArn         string `keda:"name=awsRoleArn,          order=authParams, optional"`
	IdentityOwner      string `keda:"name=identityOwner,       order=triggerMetadata, default=pod"`
}

func (c *AwsAuthConfig) Validate() error {
	if c.IdentityOwner != "" && c.IdentityOwner != "pod" && c.IdentityOwner != "operator" {
		return fmt.Errorf("identityOwner must be either 'pod' or 'operator', got: %s", c.IdentityOwner)
	}

	// If using static credentials, both access key and secret key must be provided
	if c.AwsAccessKeyID != "" && c.AwsSecretAccessKey == "" {
		return fmt.Errorf("awsSecretAccessKey not found")
	}

	if c.AwsSecretAccessKey != "" && c.AwsAccessKeyID == "" {
		return ErrAwsNoAccessKey
	}

	return nil
}

// GetAwsConfig returns an *aws.Config for a given AuthorizationMetadata
// If AuthorizationMetadata uses static credentials or `aws` auth,
// we recover the *aws.Config from the shared cache. If not, we generate
// a new entry on each request
func GetAwsConfig(ctx context.Context, awsAuthorization AuthorizationMetadata) (*aws.Config, error) {
	if awsAuthorization.UsingPodIdentity ||
		(awsAuthorization.AwsAccessKeyID != "" && awsAuthorization.AwsSecretAccessKey != "") {
		return awsSharedCredentialsCache.GetCredentials(ctx, awsAuthorization)
	}

	// TODO, remove when aws-eks are removed
	configOptions := make([]func(*config.LoadOptions) error, 0)
	configOptions = append(configOptions, config.WithRegion(awsAuthorization.AwsRegion))
	cfg, err := config.LoadDefaultConfig(ctx, configOptions...)
	if err != nil {
		return nil, err
	}

	if !awsAuthorization.PodIdentityOwner {
		return &cfg, nil
	}

	if awsAuthorization.AwsRoleArn != "" {
		stsSvc := sts.NewFromConfig(cfg)
		stsCredentialProvider := stscreds.NewAssumeRoleProvider(stsSvc, awsAuthorization.AwsRoleArn, func(_ *stscreds.AssumeRoleOptions) {})
		cfg.Credentials = aws.NewCredentialsCache(stsCredentialProvider)
	}
	return &cfg, err
	// END remove when aws-eks are removed
}

// GetAwsAuthorization returns an AuthorizationMetadata based on trigger information
func GetAwsAuthorization(uniqueKey, awsRegion string, podIdentity kedav1alpha1.AuthPodIdentity, triggerMetadata, authParams, resolvedEnv map[string]string) (AuthorizationMetadata, error) {
	meta := AuthorizationMetadata{
		TriggerUniqueKey: uniqueKey,
		AwsRegion:        awsRegion,
	}

	if podIdentity.Provider == kedav1alpha1.PodIdentityProviderAws {
		meta.UsingPodIdentity = true
		if val, ok := authParams["awsRoleArn"]; ok && val != "" {
			meta.AwsRoleArn = val
		}
		return meta, nil
	}

	// TODO, remove all the logic below and just keep the logic for
	// parsing awsAccessKeyID, awsSecretAccessKey and awsSessionToken
	// when aws-eks are removed
	switch triggerMetadata["identityOwner"] {
	case "operator":
		meta.PodIdentityOwner = false
	case "", "pod":
		meta.PodIdentityOwner = true
		switch {
		case authParams["awsRoleArn"] != "":
			meta.AwsRoleArn = authParams["awsRoleArn"]
		case (authParams["awsAccessKeyID"] != "" || authParams["awsAccessKeyId"] != "") && authParams["awsSecretAccessKey"] != "":
			meta.AwsAccessKeyID = authParams["awsAccessKeyID"]
			if meta.AwsAccessKeyID == "" {
				meta.AwsAccessKeyID = authParams["awsAccessKeyId"]
			}
			meta.AwsSecretAccessKey = authParams["awsSecretAccessKey"]
			meta.AwsSessionToken = authParams["awsSessionToken"]
		default:
			if triggerMetadata["awsAccessKeyID"] != "" {
				meta.AwsAccessKeyID = triggerMetadata["awsAccessKeyID"]
			} else if triggerMetadata["awsAccessKeyIDFromEnv"] != "" {
				meta.AwsAccessKeyID = resolvedEnv[triggerMetadata["awsAccessKeyIDFromEnv"]]
			}

			if len(meta.AwsAccessKeyID) == 0 {
				return meta, ErrAwsNoAccessKey
			}

			if triggerMetadata["awsSecretAccessKeyFromEnv"] != "" {
				meta.AwsSecretAccessKey = resolvedEnv[triggerMetadata["awsSecretAccessKeyFromEnv"]]
			}

			if len(meta.AwsSecretAccessKey) == 0 {
				return meta, fmt.Errorf("awsSecretAccessKey not found")
			}
		}
	}

	return meta, nil
}

// GetAwsAuthorizationWithAuthConfig returns an AuthorizationMetadata based on AwsAuthConfig
func GetAwsAuthorizationWithAuthConfig(uniqueKey string, podIdentity kedav1alpha1.AuthPodIdentity, authConfig *AwsAuthConfig) (AuthorizationMetadata, error) {
	meta := AuthorizationMetadata{
		TriggerUniqueKey: uniqueKey,
		AwsRegion:        authConfig.AwsRegion,
	}

	if podIdentity.Provider == kedav1alpha1.PodIdentityProviderAws {
		meta.UsingPodIdentity = true
		if authConfig.AwsRoleArn != "" {
			meta.AwsRoleArn = authConfig.AwsRoleArn
		}
		return meta, nil
	}

	// TODO, remove all the logic below and just keep the logic for
	// parsing awsAccessKeyID, awsSecretAccessKey and awsSessionToken
	// when aws-eks are removed
	switch authConfig.IdentityOwner {
	case "operator":
		meta.PodIdentityOwner = false
	case "", "pod":
		meta.PodIdentityOwner = true

		// Handle AwsRoleArn
		if authConfig.AwsRoleArn != "" {
			meta.AwsRoleArn = authConfig.AwsRoleArn
			return meta, nil
		}

		// Handle static credentials
		if authConfig.AwsAccessKeyID != "" && authConfig.AwsSecretAccessKey != "" {
			meta.AwsAccessKeyID = authConfig.AwsAccessKeyID
			meta.AwsSecretAccessKey = authConfig.AwsSecretAccessKey
			meta.AwsSessionToken = authConfig.AwsSessionToken
			return meta, nil
		}

		// If no credentials provided, return error
		if authConfig.AwsAccessKeyID == "" {
			return meta, ErrAwsNoAccessKey
		}

		if authConfig.AwsSecretAccessKey == "" {
			return meta, fmt.Errorf("awsSecretAccessKey not found")
		}
	}

	return meta, nil
}

// ClearAwsConfig wraps the removal of the config from the cache
func ClearAwsConfig(awsAuthorization AuthorizationMetadata) {
	awsSharedCredentialsCache.RemoveCachedEntry(awsAuthorization)
}
