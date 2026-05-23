/*
Copyright 2026 The KEDA Authors

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

package scaling

import (
	"context"
	"strings"
	"testing"

	"github.com/kedacore/keda/v2/pkg/scalerfilter"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

func TestBuildScaler_AllowListGates(t *testing.T) {
	t.Cleanup(func() { scalerfilter.Set(nil) })

	scalerfilter.Set([]string{"cpu"})

	_, err := buildScaler(context.Background(), nil, "memory", &scalersconfig.ScalerConfig{})
	if err == nil {
		t.Fatal("expected error for disabled scaler type, got nil")
	}
	if !strings.Contains(err.Error(), "disabled on this KEDA instance") {
		t.Fatalf("expected allow-list error, got: %v", err)
	}
}

func TestBuildScaler_NilAllowsEverything(t *testing.T) {
	t.Cleanup(func() { scalerfilter.Set(nil) })

	scalerfilter.Set(nil)

	_, err := buildScaler(context.Background(), nil, "totally-bogus-trigger-type", &scalersconfig.ScalerConfig{})
	if err == nil {
		t.Fatal("expected error from switch default for unknown type, got nil")
	}
	if strings.Contains(err.Error(), "disabled on this KEDA instance") {
		t.Fatalf("allow-list gate fired when it should be disabled: %v", err)
	}
	if !strings.Contains(err.Error(), "no scaler found for type") {
		t.Fatalf("expected default-case error, got: %v", err)
	}
}

func TestBuildScaler_EmptySliceClearsAllowList(t *testing.T) {
	t.Cleanup(func() { scalerfilter.Set(nil) })

	scalerfilter.Set([]string{"cpu"})
	scalerfilter.Set([]string{})

	_, err := buildScaler(context.Background(), nil, "totally-bogus-trigger-type", &scalersconfig.ScalerConfig{})
	if err == nil {
		t.Fatal("expected default-case error, got nil")
	}
	if strings.Contains(err.Error(), "disabled on this KEDA instance") {
		t.Fatalf("empty slice did not clear allow-list: %v", err)
	}
}

func TestBuildScaler_TypeInAllowListPassesGate(t *testing.T) {
	t.Cleanup(func() { scalerfilter.Set(nil) })

	scalerfilter.Set([]string{"some-unknown-type"})

	_, err := buildScaler(context.Background(), nil, "some-unknown-type", &scalersconfig.ScalerConfig{})
	if err == nil {
		t.Fatal("expected default-case error from switch, got nil")
	}
	if strings.Contains(err.Error(), "disabled on this KEDA instance") {
		t.Fatalf("gate blocked a type that was in the allow-list: %v", err)
	}
}
