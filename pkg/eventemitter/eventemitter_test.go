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

package eventemitter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	eventingv1alpha1 "github.com/kedacore/keda/v2/apis/eventing/v1alpha1"
	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/eventemitter/eventdata"
	"github.com/kedacore/keda/v2/pkg/mock/mock_client"
	"github.com/kedacore/keda/v2/pkg/mock/mock_eventemitter"
)

const testNamespaceGlobal = "testNamespace"
const testNameGlobal = "testName"

func TestEventHandler_FailedEmitEvent(t *testing.T) {
	cloudEventSourceName := testNameGlobal
	cloudEventSourceNamespace := testNamespaceGlobal

	ctrl := gomock.NewController(t)
	recorder := record.NewFakeRecorder(1)
	mockClient := mock_client.NewMockClient(ctrl)
	eventHandler := mock_eventemitter.NewMockEventDataHandler(ctrl)
	cloudEventSource := eventingv1alpha1.CloudEventSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cloudEventSourceName,
			Namespace: cloudEventSourceNamespace,
		},
		Spec: eventingv1alpha1.CloudEventSourceSpec{
			Destination: eventingv1alpha1.Destination{
				HTTP: &eventingv1alpha1.CloudEventHTTP{
					URI: "http://fo.wo",
				},
			},
		},
		Status: eventingv1alpha1.CloudEventSourceStatus{
			Conditions: kedav1alpha1.Conditions{{Type: kedav1alpha1.ConditionActive, Status: metav1.ConditionTrue}},
		},
	}

	caches := map[string]EventDataHandler{}
	key := newEventHandlerKey(cloudEventSource.GenerateIdentifier(), cloudEventHandlerTypeHTTP)
	caches[key] = eventHandler

	filtercaches := map[string]*EventFilter{}

	eventEmitter := EventEmitter{
		client:                   mockClient,
		recorder:                 recorder,
		clusterName:              "cluster-name",
		eventHandlersCache:       caches,
		eventHandlersCacheLock:   &sync.RWMutex{},
		eventFilterCache:         filtercaches,
		eventFilterCacheLock:     &sync.RWMutex{},
		eventLoopContexts:        &sync.Map{},
		cloudEventProcessingChan: make(chan eventdata.EventData, 1),
	}

	eventData := eventdata.EventData{
		Namespace:      "aaa",
		ObjectName:     "bbb",
		CloudEventType: "ccc",
		Reason:         "ddd",
		Message:        "eee",
		Time:           time.Now().UTC(),
	}

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eventHandler.EXPECT().GetActiveStatus().Return(metav1.ConditionTrue).AnyTimes()
	go eventEmitter.startEventLoop(context.TODO(), &cloudEventSource, &sync.Mutex{})

	time.Sleep(1 * time.Second)
	eventHandler.EXPECT().SetActiveStatus(metav1.ConditionFalse).MinTimes(1)
	eventHandler.EXPECT().EmitEvent(gomock.Any(), gomock.Any()).AnyTimes().Do(func(data eventdata.EventData, failedfunc func(eventdata.EventData, error)) {
		failedfunc(data, fmt.Errorf("testing error"))
	})
	eventEmitter.enqueueEventData(eventData)
	time.Sleep(2 * time.Second)
}

func TestEventHandler_DirectCall(t *testing.T) {
	cloudEventSourceName := testNameGlobal
	cloudEventSourceNamespace := testNamespaceGlobal

	ctrl := gomock.NewController(t)
	recorder := record.NewFakeRecorder(1)
	mockClient := mock_client.NewMockClient(ctrl)

	eventHandler := mock_eventemitter.NewMockEventDataHandler(ctrl)
	cloudEventSource := eventingv1alpha1.CloudEventSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cloudEventSourceName,
			Namespace: cloudEventSourceNamespace,
		},
		Spec: eventingv1alpha1.CloudEventSourceSpec{
			Destination: eventingv1alpha1.Destination{
				HTTP: &eventingv1alpha1.CloudEventHTTP{
					URI: "http://fo.wo",
				},
			},
		},
		Status: eventingv1alpha1.CloudEventSourceStatus{
			Conditions: kedav1alpha1.Conditions{{Type: kedav1alpha1.ConditionActive, Status: metav1.ConditionTrue}},
		},
	}

	caches := map[string]EventDataHandler{}
	key := newEventHandlerKey(cloudEventSource.GenerateIdentifier(), cloudEventHandlerTypeHTTP)
	caches[key] = eventHandler

	filtercaches := map[string]*EventFilter{}

	eventEmitter := EventEmitter{
		client:                   mockClient,
		recorder:                 recorder,
		clusterName:              "cluster-name",
		eventHandlersCache:       caches,
		eventHandlersCacheLock:   &sync.RWMutex{},
		eventFilterCache:         filtercaches,
		eventFilterCacheLock:     &sync.RWMutex{},
		eventLoopContexts:        &sync.Map{},
		cloudEventProcessingChan: make(chan eventdata.EventData, 1),
	}

	eventData := eventdata.EventData{
		Namespace:      "aaa",
		ObjectName:     "bbb",
		CloudEventType: "ccc",
		Reason:         "ddd",
		Message:        "eee",
		Time:           time.Now().UTC(),
	}

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eventHandler.EXPECT().GetActiveStatus().Return(metav1.ConditionTrue).AnyTimes()
	go eventEmitter.startEventLoop(context.TODO(), &cloudEventSource, &sync.Mutex{})

	time.Sleep(1 * time.Second)

	wg := sync.WaitGroup{}
	wg.Add(1)
	eventHandler.EXPECT().EmitEvent(gomock.Any(), gomock.Any()).Times(1).Do(func(arg0, arg1 interface{}) {
		defer wg.Done()
	})
	eventEmitter.enqueueEventData(eventData)
	wg.Wait()
}

// Test event filtering functionality
func TestEventFilter(t *testing.T) {
	cloudEventSourceName := testNameGlobal
	cloudEventSourceNamespace := testNamespaceGlobal

	ctrl := gomock.NewController(t)
	recorder := record.NewFakeRecorder(1)
	mockClient := mock_client.NewMockClient(ctrl)
	eventHandler := mock_eventemitter.NewMockEventDataHandler(ctrl)

	cloudEventSource := eventingv1alpha1.CloudEventSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cloudEventSourceName,
			Namespace: cloudEventSourceNamespace,
		},
		Spec: eventingv1alpha1.CloudEventSourceSpec{
			Destination: eventingv1alpha1.Destination{
				HTTP: &eventingv1alpha1.CloudEventHTTP{
					URI: "http://fo.wo",
				},
			},
			EventSubscription: eventingv1alpha1.EventSubscription{
				IncludedEventTypes: []eventingv1alpha1.CloudEventType{"included-type"},
				ExcludedEventTypes: []eventingv1alpha1.CloudEventType{"excluded-type"},
			},
		},
		Status: eventingv1alpha1.CloudEventSourceStatus{
			Conditions: kedav1alpha1.Conditions{{Type: kedav1alpha1.ConditionActive, Status: metav1.ConditionTrue}},
		},
	}

	caches := map[string]EventDataHandler{}
	key := newEventHandlerKey(cloudEventSource.GenerateIdentifier(), cloudEventHandlerTypeHTTP)
	caches[key] = eventHandler

	// Set up the event filter
	filtercaches := map[string]*EventFilter{}
	filtercaches[cloudEventSource.GenerateIdentifier()] = NewEventFilter(
		cloudEventSource.Spec.EventSubscription.IncludedEventTypes,
		cloudEventSource.Spec.EventSubscription.ExcludedEventTypes,
	)

	eventEmitter := EventEmitter{
		client:                   mockClient,
		recorder:                 recorder,
		clusterName:              "cluster-name",
		eventHandlersCache:       caches,
		eventHandlersCacheLock:   &sync.RWMutex{},
		eventFilterCache:         filtercaches,
		eventFilterCacheLock:     &sync.RWMutex{},
		eventLoopContexts:        &sync.Map{},
		cloudEventProcessingChan: make(chan eventdata.EventData, 3),
	}

	// We'll only test 2 event types - included and excluded
	includedEventData := eventdata.EventData{
		Namespace:      "aaa",
		ObjectName:     "bbb",
		CloudEventType: "included-type",
		Reason:         "ddd",
		Message:        "eee",
		Time:           time.Now().UTC(),
	}

	excludedEventData := eventdata.EventData{
		Namespace:      "aaa",
		ObjectName:     "bbb",
		CloudEventType: "excluded-type",
		Reason:         "ddd",
		Message:        "eee",
		Time:           time.Now().UTC(),
	}

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eventHandler.EXPECT().GetActiveStatus().Return(metav1.ConditionTrue).AnyTimes()

	// We expect only the included event to be processed
	wg := sync.WaitGroup{}
	wg.Add(1)

	// Only expect EmitEvent to be called once (for the included event)
	eventHandler.EXPECT().EmitEvent(gomock.Any(), gomock.Any()).Times(1).Do(func(data eventdata.EventData, failureFunc interface{}) {
		if data.CloudEventType != "included-type" {
			t.Errorf("Expected CloudEventType 'included-type', got '%s'", data.CloudEventType)
		}
		wg.Done()
	})

	go eventEmitter.startEventLoop(context.TODO(), &cloudEventSource, &sync.Mutex{})
	time.Sleep(1 * time.Second)

	// Send both events with a small delay between
	eventEmitter.enqueueEventData(includedEventData)
	time.Sleep(100 * time.Millisecond)
	eventEmitter.enqueueEventData(excludedEventData)

	// Wait for the included event to be processed
	if waitTimeout(&wg, 3*time.Second) {
		t.Error("Timed out waiting for events to be processed")
	}

	// Give some time for the excluded event to be processed (if it incorrectly would be)
	time.Sleep(1 * time.Second)
}

// Helper function to time out waiting for a WaitGroup
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

// Test multiple handlers scenario
func TestMultipleEventHandlers(t *testing.T) {
	cloudEventSourceName := testNameGlobal
	cloudEventSourceNamespace := testNamespaceGlobal

	ctrl := gomock.NewController(t)
	recorder := record.NewFakeRecorder(1)
	mockClient := mock_client.NewMockClient(ctrl)

	// Create two handlers
	eventHandler1 := mock_eventemitter.NewMockEventDataHandler(ctrl)
	eventHandler2 := mock_eventemitter.NewMockEventDataHandler(ctrl)

	cloudEventSource := eventingv1alpha1.CloudEventSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cloudEventSourceName,
			Namespace: cloudEventSourceNamespace,
		},
		Spec: eventingv1alpha1.CloudEventSourceSpec{
			Destination: eventingv1alpha1.Destination{
				HTTP: &eventingv1alpha1.CloudEventHTTP{
					URI: "http://fo.wo",
				},
			},
		},
		Status: eventingv1alpha1.CloudEventSourceStatus{
			Conditions: kedav1alpha1.Conditions{{Type: kedav1alpha1.ConditionActive, Status: metav1.ConditionTrue}},
		},
	}

	// Set up multiple handlers
	caches := map[string]EventDataHandler{}
	key1 := newEventHandlerKey(cloudEventSource.GenerateIdentifier(), cloudEventHandlerTypeHTTP)
	key2 := newEventHandlerKey(cloudEventSource.GenerateIdentifier(), cloudEventHandlerTypeAzureEventGridTopic) // pretend a second handler
	caches[key1] = eventHandler1
	caches[key2] = eventHandler2

	filtercaches := map[string]*EventFilter{}

	eventEmitter := EventEmitter{
		client:                   mockClient,
		recorder:                 recorder,
		clusterName:              "cluster-name",
		eventHandlersCache:       caches,
		eventHandlersCacheLock:   &sync.RWMutex{},
		eventFilterCache:         filtercaches,
		eventFilterCacheLock:     &sync.RWMutex{},
		eventLoopContexts:        &sync.Map{},
		cloudEventProcessingChan: make(chan eventdata.EventData, 1),
	}

	eventData := eventdata.EventData{
		Namespace:      "aaa",
		ObjectName:     "bbb",
		CloudEventType: "ccc",
		Reason:         "ddd",
		Message:        "eee",
		Time:           time.Now().UTC(),
	}

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eventHandler1.EXPECT().GetActiveStatus().Return(metav1.ConditionTrue).AnyTimes()
	eventHandler2.EXPECT().GetActiveStatus().Return(metav1.ConditionTrue).AnyTimes()

	go eventEmitter.startEventLoop(context.TODO(), &cloudEventSource, &sync.Mutex{})
	time.Sleep(1 * time.Second)

	// Both handlers should receive the event
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Verify both handlers get called
	eventHandler1.EXPECT().EmitEvent(gomock.Any(), gomock.Any()).Times(1).Do(func(data eventdata.EventData, failureFunc interface{}) {
		if data.HandlerKey != key1 {
			t.Errorf("Expected handler key %s, got %s", key1, data.HandlerKey)
		}
		wg.Done()
	})

	eventHandler2.EXPECT().EmitEvent(gomock.Any(), gomock.Any()).Times(1).Do(func(data eventdata.EventData, failureFunc interface{}) {
		if data.HandlerKey != key2 {
			t.Errorf("Expected handler key %s, got %s", key2, data.HandlerKey)
		}
		wg.Done()
	})

	eventEmitter.enqueueEventData(eventData)

	// Wait for both handlers to be called
	if waitTimeout(&wg, 3*time.Second) {
		t.Error("Timed out waiting for handlers to be called")
	}
}
