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

// Package scalerfilter holds the process-wide allow-list of scaler trigger
// types. It exists as a tiny standalone package so both pkg/scaling and the
// v1alpha1 admission webhook can consult the same state without an import
// cycle (pkg/scaling already depends on apis/keda/v1alpha1).
package scalerfilter

import "sync"

var (
	mu      sync.RWMutex
	allowed map[string]struct{}
)

// Set replaces the allow-list. An empty or nil slice clears the restriction
// and IsEnabled will then return true for every trigger type. Intended to be
// called once at process startup from main.
func Set(scalers []string) {
	mu.Lock()
	defer mu.Unlock()

	if len(scalers) == 0 {
		allowed = nil
		return
	}
	set := make(map[string]struct{}, len(scalers))
	for _, s := range scalers {
		set[s] = struct{}{}
	}
	allowed = set
}

// IsEnabled reports whether the given trigger type may be instantiated.
// Returns true when no allow-list is configured.
func IsEnabled(triggerType string) bool {
	mu.RLock()
	defer mu.RUnlock()

	if allowed == nil {
		return true
	}
	_, ok := allowed[triggerType]
	return ok
}
