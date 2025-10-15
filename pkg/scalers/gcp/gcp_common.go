package gcp

import (
	"context"
	"errors"
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

var (
	GcpScopeMonitoringRead = "https://www.googleapis.com/auth/monitoring.read"

	ErrGoogleApplicationCrendentialsNotFound = errors.New("google application credentials not found")
)

type AuthorizationMetadata struct {
	GoogleApplicationCredentials     string
	GoogleApplicationCredentialsFile string
	PodIdentityProviderEnabled       bool
}

type GCPAuthConfig struct {
	Credentials     string `keda:"name=credentials, order=triggerMetadata;resolvedEnv;authParams, optional"`
	CredentialsFile string `keda:"name=credentialsFile, order=triggerMetadata;resolvedEnv;authParams, optional"`

	TriggerIndex int
}

func (a *AuthorizationMetadata) tokenSource(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	if a.PodIdentityProviderEnabled {
		return google.DefaultTokenSource(ctx, scopes...)
	}

	if a.GoogleApplicationCredentials != "" {
		creds, err := google.CredentialsFromJSON(ctx, []byte(a.GoogleApplicationCredentials), scopes...)
		if err != nil {
			return nil, err
		}

		return creds.TokenSource, nil
	}

	if a.GoogleApplicationCredentialsFile != "" {
		data, err := os.ReadFile(a.GoogleApplicationCredentialsFile)
		if err != nil {
			return nil, err
		}

		creds, err := google.CredentialsFromJSON(ctx, data, scopes...)
		if err != nil {
			return nil, err
		}

		return creds.TokenSource, nil
	}

	return nil, ErrGoogleApplicationCrendentialsNotFound
}

func GetGCPAuthorization(config *scalersconfig.ScalerConfig) (*AuthorizationMetadata, error) {
	if config.PodIdentity.Provider == kedav1alpha1.PodIdentityProviderGCP {
		return &AuthorizationMetadata{PodIdentityProviderEnabled: true}, nil
	}

	gcpAuth := &GCPAuthConfig{}
	if err := config.TypedConfig(gcpAuth); err != nil {
		return nil, err
	}

	if gcpAuth.Credentials != "" {
		return &AuthorizationMetadata{GoogleApplicationCredentials: gcpAuth.Credentials}, nil
	}

	if gcpAuth.CredentialsFile != "" {
		return &AuthorizationMetadata{GoogleApplicationCredentialsFile: gcpAuth.CredentialsFile}, nil
	}

	return nil, ErrGoogleApplicationCrendentialsNotFound
}

func GetGCPOAuth2HTTPTransport(config *scalersconfig.ScalerConfig, base http.RoundTripper, scopes ...string) (http.RoundTripper, error) {
	a, err := GetGCPAuthorization(config)
	if err != nil {
		return nil, err
	}

	ts, err := a.tokenSource(context.Background(), scopes...)
	if err != nil {
		return nil, err
	}

	return &oauth2.Transport{Source: ts, Base: base}, nil
}
