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

package scalerfilter

import "testing"

func TestScalerFilter(t *testing.T) {
	t.Cleanup(func() { Set(nil) })

	t.Run("nil allows everything", func(t *testing.T) {
		Set(nil)
		if !IsEnabled("anything") {
			t.Fatal("nil allow-list should allow any scaler")
		}
	})

	t.Run("empty slice clears prior allow-list", func(t *testing.T) {
		Set([]string{"cpu"})
		Set([]string{})
		if !IsEnabled("kafka") {
			t.Fatal("empty slice should clear the allow-list")
		}
	})

	t.Run("allow-list gates", func(t *testing.T) {
		Set([]string{"cpu", "memory"})
		if !IsEnabled("cpu") {
			t.Fatal("cpu should be enabled")
		}
		if IsEnabled("kafka") {
			t.Fatal("kafka should be disabled")
		}
	})
}
