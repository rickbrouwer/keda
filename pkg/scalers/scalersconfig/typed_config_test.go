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

package scalersconfig

import (
	"net/url"
	"testing"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
)

// TestBasicTypedConfig tests the basic types for typed config
func TestBasicTypedConfig(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"stringVal":       "value1",
			"intVal":          "1",
			"boolValFromEnv":  "boolVal",
			"floatValFromEnv": "floatVal",
		},
		ResolvedEnv: map[string]string{
			"boolVal":  "true",
			"floatVal": "1.1",
		},
		AuthParams: map[string]string{
			"auth": "authValue",
		},
	}

	type testStruct struct {
		StringVal string  `keda:"name=stringVal, order=triggerMetadata"`
		IntVal    int     `keda:"name=intVal,    order=triggerMetadata"`
		BoolVal   bool    `keda:"name=boolVal,   order=resolvedEnv"`
		FloatVal  float64 `keda:"name=floatVal,  order=resolvedEnv"`
		AuthVal   string  `keda:"name=auth,      order=authParams"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())

	Expect(ts.StringVal).To(Equal("value1"))
	Expect(ts.IntVal).To(Equal(1))
	Expect(ts.BoolVal).To(BeTrue())
	Expect(ts.FloatVal).To(Equal(1.1))
	Expect(ts.AuthVal).To(Equal("authValue"))
}

// TestParsingOrder tests the parsing order
func TestParsingOrder(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"stringVal":       "value1",
			"intVal":          "1",
			"intValFromEnv":   "intVal",
			"floatVal":        "1.1",
			"floatValFromEnv": "floatVal",
		},
		ResolvedEnv: map[string]string{
			"stringVal": "value2",
			"intVal":    "2",
			"floatVal":  "2.2",
		},
	}

	type testStruct struct {
		StringVal string  `keda:"name=stringVal, order=resolvedEnv;triggerMetadata"`
		IntVal    int     `keda:"name=intVal,    order=triggerMetadata;resolvedEnv"`
		FloatVal  float64 `keda:"name=floatVal,  order=resolvedEnv;triggerMetadata"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())

	Expect(ts.StringVal).To(Equal("value1"))
	Expect(ts.IntVal).To(Equal(1))
	Expect(ts.FloatVal).To(Equal(2.2))
}

// TestOptional tests the optional tag
func TestOptional(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"stringVal": "value1",
		},
	}

	type testStruct struct {
		StringVal          string `keda:"name=stringVal, order=triggerMetadata"`
		IntValOptional     int    `keda:"name=intVal,    order=triggerMetadata, optional"`
		IntValAlsoOptional int    `keda:"name=intVal,    order=triggerMetadata, optional=true"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())

	Expect(ts.StringVal).To(Equal("value1"))
	Expect(ts.IntValOptional).To(Equal(0))
	Expect(ts.IntValAlsoOptional).To(Equal(0))
}

// TestMissing tests the missing parameter for compulsory tag
func TestMissing(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{}

	type testStruct struct {
		StringVal string `keda:"name=stringVal,  order=triggerMetadata"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(MatchError(`missing required parameter "stringVal" in [triggerMetadata]`))
}

// TestDeprecated tests the deprecated tag
func TestDeprecated(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"stringVal": "value1",
		},
	}

	type testStruct struct {
		StringVal string `keda:"name=stringVal, order=triggerMetadata, deprecated=deprecated"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(MatchError(`parameter "stringVal" is deprecated`))

	sc2 := &ScalerConfig{
		TriggerMetadata: map[string]string{},
	}

	ts2 := testStruct{}
	err = sc2.TypedConfig(&ts2)
	Expect(err).To(BeNil())
}

// TestDefaultValue tests the default tag
func TestDefaultValue(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"stringVal": "value1",
		},
	}

	type testStruct struct {
		BoolVal    bool   `keda:"name=boolVal,    order=triggerMetadata, optional, default=true"`
		StringVal  string `keda:"name=stringVal,  order=triggerMetadata, optional, default=d"`
		StringVal2 string `keda:"name=stringVal2, order=triggerMetadata, optional, default=d"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())

	Expect(ts.BoolVal).To(Equal(true))
	Expect(ts.StringVal).To(Equal("value1"))
	Expect(ts.StringVal2).To(Equal("d"))
}

// TestMap tests the map type
func TestMap(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"mapVal": "key1=1,key2=2",
		},
	}

	type testStruct struct {
		MapVal map[string]int `keda:"name=mapVal, order=triggerMetadata"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.MapVal).To(HaveLen(2))
	Expect(ts.MapVal["key1"]).To(Equal(1))
	Expect(ts.MapVal["key2"]).To(Equal(2))
}

// TestSlice tests the slice type
func TestSlice(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"sliceVal":                   "1,2,3",
			"sliceValWithSpaces":         "1, 2, 3",
			"sliceValWithOtherSeparator": "1;2;3",
		},
	}

	type testStruct struct {
		SliceVal                   []int `keda:"name=sliceVal, order=triggerMetadata"`
		SliceValWithSpaces         []int `keda:"name=sliceValWithSpaces, order=triggerMetadata"`
		SliceValWithOtherSeparator []int `keda:"name=sliceValWithOtherSeparator, order=triggerMetadata, separator=;"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.SliceVal).To(HaveLen(3))
	Expect(ts.SliceVal[0]).To(Equal(1))
	Expect(ts.SliceVal[1]).To(Equal(2))
	Expect(ts.SliceVal[2]).To(Equal(3))
	Expect(ts.SliceValWithSpaces).To(HaveLen(3))
	Expect(ts.SliceValWithSpaces[0]).To(Equal(1))
	Expect(ts.SliceValWithSpaces[1]).To(Equal(2))
	Expect(ts.SliceValWithSpaces[2]).To(Equal(3))
	Expect(ts.SliceValWithOtherSeparator).To(HaveLen(3))
	Expect(ts.SliceValWithOtherSeparator[0]).To(Equal(1))
	Expect(ts.SliceValWithOtherSeparator[1]).To(Equal(2))
	Expect(ts.SliceValWithOtherSeparator[2]).To(Equal(3))
}

// TestEnum tests the enum type
func TestEnum(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"enumVal":   "value1",
			"enumSlice": "value1, value2",
		},
	}

	type testStruct struct {
		EnumVal   string   `keda:"name=enumVal,   order=triggerMetadata, enum=value1;value2"`
		EnumSlice []string `keda:"name=enumSlice, order=triggerMetadata, enum=value1;value2, optional"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.EnumVal).To(Equal("value1"))
	Expect(ts.EnumSlice).To(HaveLen(2))
	Expect(ts.EnumSlice).To(ConsistOf("value1", "value2"))

	sc2 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"enumVal": "value3",
		},
	}

	ts2 := testStruct{}
	err = sc2.TypedConfig(&ts2)
	Expect(err).To(MatchError(`parameter "enumVal" value "value3" must be one of [value1 value2]`))
}

// TestExclusive tests the exclusiveSet type
func TestExclusive(t *testing.T) {
	RegisterTestingT(t)

	type testStruct struct {
		IntVal []int `keda:"name=intVal, order=triggerMetadata, exclusiveSet=1;4;5"`
	}

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"intVal": "1,2,3",
		},
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())

	sc2 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"intVal": "1,4",
		},
	}

	ts2 := testStruct{}
	err = sc2.TypedConfig(&ts2)
	Expect(err).To(MatchError(`parameter "intVal" value "1,4" must contain only one of [1 4 5]`))
}

// TestURLValues tests the url.Values type
func TestURLValues(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		AuthParams: map[string]string{
			"endpointParams": "key1=value1&key2=value2&key1=value3",
		},
	}

	type testStruct struct {
		EndpointParams url.Values `keda:"name=endpointParams, order=authParams"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.EndpointParams).To(HaveLen(2))
	Expect(ts.EndpointParams).To(HaveKey("key1"))
	Expect(ts.EndpointParams).To(HaveKey("key2"))
	Expect(ts.EndpointParams["key1"]).To(ConsistOf("value1", "value3"))
	Expect(ts.EndpointParams["key2"]).To(ConsistOf("value2"))
}

// TestGenericMap tests the generic map type that is structurally similar to url.Values
func TestGenericMap(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		AuthParams: map[string]string{
			"endpointParams": "key1=value1,key2=value2,key3=value3",
		},
	}

	// structurally similar to url.Values but should behave as generic map
	type testStruct struct {
		EndpointParams map[string][]string `keda:"name=endpointParams, order=authParams"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.EndpointParams).To(HaveLen(3))
	Expect(ts.EndpointParams).To(HaveKey("key1"))
	Expect(ts.EndpointParams).To(HaveKey("key2"))
	Expect(ts.EndpointParams).To(HaveKey("key3"))
	Expect(ts.EndpointParams["key1"]).To(ConsistOf("value1"))
	Expect(ts.EndpointParams["key2"]).To(ConsistOf("value2"))
	Expect(ts.EndpointParams["key3"]).To(ConsistOf("value3"))
}

// TestNestedStruct tests the nested struct type
func TestNestedStruct(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		AuthParams: map[string]string{
			"username": "user",
			"password": "pass",
		},
	}

	type basicAuth struct {
		Username string `keda:"name=username, order=authParams"`
		Password string `keda:"name=password, order=authParams"`
	}

	type testStruct struct {
		BA basicAuth `keda:""`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.BA.Username).To(Equal("user"))
	Expect(ts.BA.Password).To(Equal("pass"))
}

// TestEmbeddedStruct tests the embedded struct type
func TestEmbeddedStruct(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		AuthParams: map[string]string{
			"username": "user",
			"password": "pass",
		},
	}

	type testStruct struct {
		BasicAuth struct {
			Username string `keda:"name=username, order=authParams"`
			Password string `keda:"name=password, order=authParams"`
		} `keda:""`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.BasicAuth.Username).To(Equal("user"))
	Expect(ts.BasicAuth.Password).To(Equal("pass"))
}

// TestWrongNestedStruct tests the wrong nested type
func TestWrongNestedStruct(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		AuthParams: map[string]string{
			"username": "user",
			"password": "pass",
		},
	}

	type testStruct struct {
		WrongNesting int `keda:""`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(MatchError(`nested parameter "WrongNesting" must be a struct, has kind "int"`))
}

// TestNestedOptional tests the nested optional type
func TestNestedOptional(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		AuthParams: map[string]string{
			"username": "user",
		},
	}

	type basicAuth struct {
		Username                   string `keda:"name=username, order=authParams"`
		Password                   string `keda:"name=password, order=authParams, optional"`
		AlsoOptionalThanksToParent string `keda:"name=optional, order=authParams"`
	}

	type testStruct struct {
		BA basicAuth `keda:"optional"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.BA.Username).To(Equal("user"))
	Expect(ts.BA.Password).To(Equal(""))
	Expect(ts.BA.AlsoOptionalThanksToParent).To(Equal(""))
}

// TestNestedPointer tests the nested pointer type
func TestNestedPointer(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		AuthParams: map[string]string{
			"username": "user",
			"password": "pass",
		},
	}

	type basicAuth struct {
		Username string `keda:"name=username, order=authParams"`
		Password string `keda:"name=password, order=authParams"`
	}

	type testStruct struct {
		BA *basicAuth `keda:""`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.BA).ToNot(BeNil())
	Expect(ts.BA.Username).To(Equal("user"))
	Expect(ts.BA.Password).To(Equal("pass"))
}

// TestNoParsingOrder tests when no parsing order is provided
func TestNoParsingOrder(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"strVal":     "value1",
			"defaultVal": "value2",
		},
	}

	type testStructMissing struct {
		StrVal string `keda:"name=strVal, enum=value1;value2"`
	}
	tsm := testStructMissing{}
	err := sc.TypedConfig(&tsm)
	Expect(err).To(MatchError(ContainSubstring(`missing required parameter "strVal", no 'order' tag, provide any from [authParams resolvedEnv triggerMetadata]`)))

	type testStructDefault struct {
		DefaultVal string `keda:"name=defaultVal, default=dv"`
	}
	tsd := testStructDefault{}
	err = sc.TypedConfig(&tsd)
	Expect(err).To(BeNil())
	Expect(tsd.DefaultVal).To(Equal("dv"))

	type testStructDefaultMissing struct {
		DefaultVal2 string `keda:"name=defaultVal2, default=dv"`
	}
	tsdm := testStructDefaultMissing{}
	err = sc.TypedConfig(&tsdm)
	Expect(err).To(BeNil())
	Expect(tsdm.DefaultVal2).To(Equal("dv"))
}

// TestRange tests the range param
func TestRange(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"range":       "5-10",
			"multiRange":  "5-10, 15-20",
			"dottedRange": "2..7",
			"wrongRange":  "5..3",
		},
	}

	type testStruct struct {
		Range       []int `keda:"name=range,       order=triggerMetadata, range=-"`
		MultiRange  []int `keda:"name=multiRange,  order=triggerMetadata, range"`
		DottedRange []int `keda:"name=dottedRange, order=triggerMetadata, range=.."`
		WrongRange  []int `keda:"name=wrongRange,  order=triggerMetadata, range=.."`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.Range).To(HaveLen(6))
	Expect(ts.Range).To(ConsistOf(5, 6, 7, 8, 9, 10))
	Expect(ts.MultiRange).To(HaveLen(12))
	Expect(ts.MultiRange).To(ConsistOf(5, 6, 7, 8, 9, 10, 15, 16, 17, 18, 19, 20))
	Expect(ts.DottedRange).To(HaveLen(6))
	Expect(ts.DottedRange).To(ConsistOf(2, 3, 4, 5, 6, 7))
	Expect(ts.WrongRange).To(HaveLen(0))
}

// TestMultiName tests the multi name param
func TestMultiName(t *testing.T) {
	RegisterTestingT(t)

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"property1": "aaa",
		},
	}

	sc2 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"property2": "bbb",
		},
	}

	type testStruct struct {
		Property string `keda:"name=property1;property2, order=triggerMetadata"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.Property).To(Equal("aaa"))

	err = sc2.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.Property).To(Equal("bbb"))
}

// TestDeprecatedAnnounce tests the deprecatedAnnounce tag
func TestDeprecatedAnnounce(t *testing.T) {
	RegisterTestingT(t)

	// Create a mock recorder to capture the event
	mockRecorder := &MockEventRecorder{}

	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"oldParam": "value1",
		},
		Recorder: mockRecorder,
	}

	type testStruct struct {
		OldParam string `keda:"name=oldParam, order=triggerMetadata, deprecatedAnnounce=This parameter is deprecated. Use newParam instead"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.OldParam).To(Equal("value1"))

	// Verify that the deprecation event was recorded
	Expect(mockRecorder.EventCalled).To(BeTrue())
	Expect(mockRecorder.Message).To(Equal("Scaler  info: This parameter is deprecated. Use newParam instead"))
}

// TestMinValue tests the minValue tag for both inclusive and exclusive scenarios
func TestMinValue(t *testing.T) {
	RegisterTestingT(t)

	// Test with valid values
	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"inclusiveMin":        "0",   // exact value, inclusive (>=0)
			"inclusiveMinAbove":   "5",   // above min value, inclusive (>=0)
			"exclusiveMin":        "1",   // above min value, exclusive (>0)
			"floatInclusiveMin":   "2.5", // exact value, inclusive (>=2.5)
			"floatInclusiveAbove": "3.5", // above min value, inclusive (>=2.5)
			"floatExclusiveMin":   "0.1", // above min value, exclusive (>0)
		},
	}

	type testStruct struct {
		InclusiveMin        int     `keda:"name=inclusiveMin, order=triggerMetadata, minValue=0"`
		InclusiveMinAbove   int     `keda:"name=inclusiveMinAbove, order=triggerMetadata, minValue=0"`
		ExclusiveMin        int     `keda:"name=exclusiveMin, order=triggerMetadata, minValue=>0"`
		FloatInclusiveMin   float64 `keda:"name=floatInclusiveMin, order=triggerMetadata, minValue=2.5"`
		FloatInclusiveAbove float64 `keda:"name=floatInclusiveAbove, order=triggerMetadata, minValue=2.5"`
		FloatExclusiveMin   float64 `keda:"name=floatExclusiveMin, order=triggerMetadata, minValue=>0"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.InclusiveMin).To(Equal(0))
	Expect(ts.InclusiveMinAbove).To(Equal(5))
	Expect(ts.ExclusiveMin).To(Equal(1))
	Expect(ts.FloatInclusiveMin).To(Equal(2.5))
	Expect(ts.FloatInclusiveAbove).To(Equal(3.5))
	Expect(ts.FloatExclusiveMin).To(Equal(0.1))

	// Test with invalid inclusive minimum value
	sc2 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"inclusiveMin": "-1", // below min value, inclusive (>=0)
			// We need to add all other required fields to make the test pass
			"inclusiveMinAbove":   "5",
			"exclusiveMin":        "1",
			"floatInclusiveMin":   "2.5",
			"floatInclusiveAbove": "3.5",
			"floatExclusiveMin":   "0.1",
		},
	}

	ts2 := testStruct{}
	err = sc2.TypedConfig(&ts2)
	Expect(err).To(MatchError(`parameter "inclusiveMin" must be greater than or equal to 0, got -1`))

	// Test with invalid exclusive minimum value
	sc3 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"inclusiveMin":        "0",
			"inclusiveMinAbove":   "5",
			"exclusiveMin":        "0", // exact min value, exclusive (>0)
			"floatInclusiveMin":   "2.5",
			"floatInclusiveAbove": "3.5",
			"floatExclusiveMin":   "0.1",
		},
	}

	ts3 := testStruct{}
	err = sc3.TypedConfig(&ts3)
	Expect(err).To(MatchError(`parameter "exclusiveMin" must be greater than 0, got 0`))

	// Test with invalid inclusive float minimum value
	sc4 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"inclusiveMin":        "0",
			"inclusiveMinAbove":   "5",
			"exclusiveMin":        "1",
			"floatInclusiveMin":   "2.4", // below min value, inclusive (>=2.5)
			"floatInclusiveAbove": "3.5",
			"floatExclusiveMin":   "0.1",
		},
	}

	ts4 := testStruct{}
	err = sc4.TypedConfig(&ts4)
	Expect(err).To(MatchError(`parameter "floatInclusiveMin" must be greater than or equal to 2.5, got 2.4`))

	// Test with invalid exclusive float minimum value
	sc5 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"inclusiveMin":        "0",
			"inclusiveMinAbove":   "5",
			"exclusiveMin":        "1",
			"floatInclusiveMin":   "2.5",
			"floatInclusiveAbove": "3.5",
			"floatExclusiveMin":   "0", // exact min value, exclusive (>0)
		},
	}

	ts5 := testStruct{}
	err = sc5.TypedConfig(&ts5)
	Expect(err).To(MatchError(`parameter "floatExclusiveMin" must be greater than 0, got 0`))
}

// TestCombinedValidation tests combining minValue with other validations
func TestCombinedValidation(t *testing.T) {
	RegisterTestingT(t)

	// Test with valid values
	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"validVal": "5",
		},
	}

	type testStruct struct {
		// This field has multiple validations (inclusive minValue and enum)
		ValidVal int `keda:"name=validVal, order=triggerMetadata, minValue=1, enum=5;10;15"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.ValidVal).To(Equal(5))

	// Test with exclusive minValue and enum
	type testExclusiveStruct struct {
		// This field has multiple validations (exclusive minValue and enum)
		ValidVal int `keda:"name=validVal, order=triggerMetadata, minValue=>0, enum=5;10;15"`
	}

	ts2 := testExclusiveStruct{}
	err = sc.TypedConfig(&ts2)
	Expect(err).To(BeNil())
	Expect(ts2.ValidVal).To(Equal(5))

	// Test with value failing enum validation
	sc2 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"validVal": "6", // Valid for minValue but not in enum
		},
	}

	ts3 := testStruct{}
	err = sc2.TypedConfig(&ts3)
	Expect(err).To(MatchError(`parameter "validVal" value "6" must be one of [5 10 15]`))

	// Test with value failing minValue validation
	sc3 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"validVal": "0", // Invalid for minValue=1
		},
	}

	ts4 := testStruct{}
	err = sc3.TypedConfig(&ts4)
	Expect(err).To(MatchError(`parameter "validVal" must be greater than or equal to 1, got 0`))
}

// TestValidateMinValueWithDefault tests minValue validation with default values
func TestValidateMinValueWithDefault(t *testing.T) {
	RegisterTestingT(t)

	// Empty trigger metadata, default values should be used and validated
	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{},
	}

	// Test with valid default values
	type testValidStruct struct {
		// Valid default that passes inclusive validation
		ValidDefault int `keda:"name=validDefault, order=triggerMetadata, optional, default=5, minValue=1"`

		// Valid default that passes exclusive validation
		ValidExclusiveDefault int `keda:"name=validExclusiveDefault, order=triggerMetadata, optional, default=1, minValue=>0"`
	}

	tsValid := testValidStruct{}
	err := sc.TypedConfig(&tsValid)
	Expect(err).To(BeNil())
	Expect(tsValid.ValidDefault).To(Equal(5))
	Expect(tsValid.ValidExclusiveDefault).To(Equal(1))

	// Test with invalid default values for inclusive minValue
	type testInvalidStruct struct {
		// Invalid default that fails inclusive validation
		InvalidDefault int `keda:"name=invalidDefault, order=triggerMetadata, optional, default=0, minValue=1"`
	}

	tsInvalid := testInvalidStruct{}
	err = sc.TypedConfig(&tsInvalid)
	Expect(err).To(MatchError(`parameter "invalidDefault" must be greater than or equal to 1, got 0`))

	// Test with invalid default values for exclusive minValue
	type testInvalidExclusiveStruct struct {
		// Invalid default that fails exclusive validation
		InvalidExclusiveDefault int `keda:"name=invalidExclusiveDefault, order=triggerMetadata, optional, default=0, minValue=>0"`
	}

	tsInvalidExclusive := testInvalidExclusiveStruct{}
	err = sc.TypedConfig(&tsInvalidExclusive)
	Expect(err).To(MatchError(`parameter "invalidExclusiveDefault" must be greater than 0, got 0`))
}

// TestOptionalMinValue tests minValue validation with optional fields
func TestOptionalMinValue(t *testing.T) {
	RegisterTestingT(t)

	// Test with valid values for optional fields
	sc := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"optionalInclusiveMin": "5", // Valid for inclusive minValue=0
			"optionalExclusiveMin": "1", // Valid for exclusive minValue=>0
		},
	}

	type testStruct struct {
		// These fields are optional but when provided should meet min requirements
		OptionalInclusiveMin int `keda:"name=optionalInclusiveMin, order=triggerMetadata, optional, minValue=0"`
		OptionalExclusiveMin int `keda:"name=optionalExclusiveMin, order=triggerMetadata, optional, minValue=>0"`
		MissingOptionalMin   int `keda:"name=missingOptionalMin, order=triggerMetadata, optional, minValue=0"`
	}

	ts := testStruct{}
	err := sc.TypedConfig(&ts)
	Expect(err).To(BeNil())
	Expect(ts.OptionalInclusiveMin).To(Equal(5))
	Expect(ts.OptionalExclusiveMin).To(Equal(1))
	Expect(ts.MissingOptionalMin).To(Equal(0)) // Zero value because field is missing and optional

	// Test with invalid values for optional fields with inclusive minValue
	sc2 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"optionalInclusiveMin": "-1", // Invalid for inclusive minValue=0
		},
	}

	ts2 := testStruct{}
	err = sc2.TypedConfig(&ts2)
	Expect(err).To(MatchError(`parameter "optionalInclusiveMin" must be greater than or equal to 0, got -1`))

	// Test with invalid values for optional fields with exclusive minValue
	sc3 := &ScalerConfig{
		TriggerMetadata: map[string]string{
			"optionalExclusiveMin": "0", // Invalid for exclusive minValue=>0
		},
	}

	ts3 := testStruct{}
	err = sc3.TypedConfig(&ts3)
	Expect(err).To(MatchError(`parameter "optionalExclusiveMin" must be greater than 0, got 0`))
}

// MockEventRecorder is a mock implementation of record.EventRecorder
type MockEventRecorder struct {
	EventCalled bool
	Message     string
}

func (m *MockEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	m.EventCalled = true
	m.Message = message
}

func (m *MockEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	// Not needed
}

func (m *MockEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	// Not needed
}
