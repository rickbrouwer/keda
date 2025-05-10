/*
Copyright 2023 The KEDA Authors

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

// ******************************* DESCRIPTION ****************************** \\
// eventemitter package describes functions that manage different CloudEventSource
// handlers and emit KEDA events to different CloudEventSource destinations through
// these handlers. A loop will be launched to monitor whether there is a new
// KEDA event once a valid CloudEventSource CRD is created. And then the eventemitter
// will send the event data to all event handlers when a new KEDA event reached.
// ************************************************************************** \\

package eventemitter

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	eventingv1alpha1 "github.com/kedacore/keda/v2/apis/eventing/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/eventemitter/eventdata"
	"github.com/kedacore/keda/v2/pkg/metricscollector"
	"github.com/kedacore/keda/v2/pkg/scalers/authentication"
	"github.com/kedacore/keda/v2/pkg/scaling/resolver"
	kedastatus "github.com/kedacore/keda/v2/pkg/status"
)

// Configuration constants
const (
	maxRetryTimes         = 5
	maxChannelBuffer      = 1024
	maxWaitingEnqueueTime = 10 * time.Second
)

// EventEmitter is the main struct for eventemitter package
type EventEmitter struct {
	log                      logr.Logger
	client                   client.Client
	recorder                 record.EventRecorder
	clusterName              string
	eventHandlersCache       map[string]EventDataHandler
	eventFilterCache         map[string]*EventFilter
	eventHandlersCacheLock   *sync.RWMutex
	eventFilterCacheLock     *sync.RWMutex
	eventLoopContexts        *sync.Map
	cloudEventProcessingChan chan eventdata.EventData
	authClientSet            *authentication.AuthClientSet
}

// EventHandler defines the behavior for EventEmitter clients
type EventHandler interface {
	DeleteCloudEventSource(cloudEventSource eventingv1alpha1.CloudEventSourceInterface) error
	HandleCloudEventSource(ctx context.Context, cloudEventSource eventingv1alpha1.CloudEventSourceInterface) error
	Emit(object runtime.Object, namespace string, eventType string, cloudeventType eventingv1alpha1.CloudEventType, reason string, message string)
}

// EventDataHandler defines the behavior for different event handlers
type EventDataHandler interface {
	EmitEvent(eventData eventdata.EventData, failureFunc func(eventData eventdata.EventData, err error))
	SetActiveStatus(status metav1.ConditionStatus)
	GetActiveStatus() metav1.ConditionStatus
	CloseHandler()
}

// EmitData defines the data structure for emitting event
type EmitData struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

const (
	cloudEventHandlerTypeHTTP                = "http"
	cloudEventHandlerTypeAzureEventGridTopic = "azureEventGridTopic"
)

// NewEventEmitter creates a new EventEmitter
func NewEventEmitter(client client.Client, recorder record.EventRecorder, clusterName string, authClientSet *authentication.AuthClientSet) EventHandler {
	return &EventEmitter{
		log:                      logf.Log.WithName("event_emitter"),
		client:                   client,
		recorder:                 recorder,
		clusterName:              clusterName,
		eventHandlersCache:       map[string]EventDataHandler{},
		eventFilterCache:         map[string]*EventFilter{},
		eventHandlersCacheLock:   &sync.RWMutex{},
		eventFilterCacheLock:     &sync.RWMutex{},
		eventLoopContexts:        &sync.Map{},
		cloudEventProcessingChan: make(chan eventdata.EventData, maxChannelBuffer),
		authClientSet:            authClientSet,
	}
}

// initializeLogger creates a logger with appropriate context
func initializeLogger(cloudEventSourceI eventingv1alpha1.CloudEventSourceInterface, cloudEventSourceEmitterName string) logr.Logger {
	return logf.Log.WithName(cloudEventSourceEmitterName).WithValues(
		"type", cloudEventSourceI.GetObjectKind(),
		"namespace", cloudEventSourceI.GetNamespace(),
		"name", cloudEventSourceI.GetName(),
	)
}

// HandleCloudEventSource will create CloudEventSource handlers that defined in spec and start an event loop once handlers
// are created successfully.
func (e *EventEmitter) HandleCloudEventSource(ctx context.Context, cloudEventSourceI eventingv1alpha1.CloudEventSourceInterface) error {
	e.createEventHandlers(ctx, cloudEventSourceI)

	if !e.checkIfEventHandlersExist(cloudEventSourceI) {
		return fmt.Errorf("no CloudEventSource handler is created for %s/%s", cloudEventSourceI.GetNamespace(), cloudEventSourceI.GetName())
	}

	key := cloudEventSourceI.GenerateIdentifier()
	cancelCtx, cancel := context.WithCancel(ctx)

	// Cancel the outdated EventLoop for the same CloudEventSource (if exists)
	value, loaded := e.eventLoopContexts.LoadOrStore(key, cancel)
	if loaded {
		if cancelValue, ok := value.(context.CancelFunc); ok {
			cancelValue()
		}
		e.eventLoopContexts.Store(key, cancel)
	} else {
		if updateErr := e.setCloudEventSourceStatusActive(ctx, cloudEventSourceI); updateErr != nil {
			e.log.Error(updateErr, "Failed to update CloudEventSource status")
			return updateErr
		}
	}

	// A mutex is used to synchronize handler per cloudEventSource
	eventingMutex := &sync.Mutex{}

	// Passing deep copy of CloudEventSource to the eventLoop go routines
	e.log.V(1).Info("Starting CloudEventSource loop", "name", cloudEventSourceI.GetName())
	go e.startEventLoop(cancelCtx, cloudEventSourceI.DeepCopyObject().(eventingv1alpha1.CloudEventSourceInterface), eventingMutex)
	return nil
}

// DeleteCloudEventSource will stop the event loop and clean event handlers in cache.
func (e *EventEmitter) DeleteCloudEventSource(cloudEventSource eventingv1alpha1.CloudEventSourceInterface) error {
	key := cloudEventSource.GenerateIdentifier()
	result, ok := e.eventLoopContexts.Load(key)
	e.log.V(1).Info("Deleting CloudEventSource", "key", key)

	if ok {
		if cancel, ok := result.(context.CancelFunc); ok {
			cancel()
		}
		e.eventLoopContexts.Delete(key)
		e.clearEventHandlersCache(cloudEventSource)
		return nil
	}

	e.log.V(1).Info("CloudEventSource was not found in controller cache", "key", key)
	return nil
}

// createEventHandlers will create different handler as defined in CloudEventSource, and store them in cache for repeated
// use in the loop.
func (e *EventEmitter) createEventHandlers(ctx context.Context, cloudEventSourceI eventingv1alpha1.CloudEventSourceInterface) {
	e.eventHandlersCacheLock.Lock()
	defer e.eventHandlersCacheLock.Unlock()
	e.eventFilterCacheLock.Lock()
	defer e.eventFilterCacheLock.Unlock()

	key := cloudEventSourceI.GenerateIdentifier()
	spec := cloudEventSourceI.GetSpec()

	clusterName := spec.ClusterName
	if clusterName == "" {
		clusterName = e.clusterName
	}

	// Resolve auth related
	authParams, podIdentity, err := resolver.ResolveAuthRefAndPodIdentity(ctx, e.client, e.log, spec.AuthenticationRef, nil, cloudEventSourceI.GetNamespace(), e.authClientSet)
	if err != nil {
		e.log.Error(err, "Error resolving auth params", "cloudEventSource", cloudEventSourceI.GetName())
		return
	}

	// Create EventFilter from CloudEventSource
	e.eventFilterCache[key] = NewEventFilter(spec.EventSubscription.IncludedEventTypes, spec.EventSubscription.ExcludedEventTypes)

	// Create different event destinations based on specification
	if spec.Destination.HTTP != nil {
		eventHandler, err := NewCloudEventHTTPHandler(ctx, clusterName, spec.Destination.HTTP.URI, initializeLogger(cloudEventSourceI, "cloudevent_http"))
		if err != nil {
			e.log.Error(err, "Failed to create CloudEvent HTTP handler")
			return
		}

		eventHandlerKey := newEventHandlerKey(key, cloudEventHandlerTypeHTTP)
		if h, ok := e.eventHandlersCache[eventHandlerKey]; ok {
			h.CloseHandler()
		}
		e.eventHandlersCache[eventHandlerKey] = eventHandler
		return
	}

	if spec.Destination.AzureEventGridTopic != nil {
		eventHandler, err := NewAzureEventGridTopicHandler(
			ctx,
			clusterName,
			spec.Destination.AzureEventGridTopic,
			authParams,
			podIdentity,
			initializeLogger(cloudEventSourceI, "azure_event_grid_topic"),
		)
		if err != nil {
			e.log.Error(err, "Failed to create Azure Event Grid handler")
			return
		}

		eventHandlerKey := newEventHandlerKey(key, cloudEventHandlerTypeAzureEventGridTopic)
		if h, ok := e.eventHandlersCache[eventHandlerKey]; ok {
			h.CloseHandler()
		}
		e.eventHandlersCache[eventHandlerKey] = eventHandler
		return
	}

	e.log.Info("No destination is defined in CloudEventSource", "CloudEventSource", cloudEventSourceI.GetName())
}

// clearEventHandlersCache will clear all event handlers that created by the passing CloudEventSource
func (e *EventEmitter) clearEventHandlersCache(cloudEventSource eventingv1alpha1.CloudEventSourceInterface) {
	e.eventHandlersCacheLock.Lock()
	defer e.eventHandlersCacheLock.Unlock()
	e.eventFilterCacheLock.Lock()
	defer e.eventFilterCacheLock.Unlock()

	spec := cloudEventSource.GetSpec()
	key := cloudEventSource.GenerateIdentifier()

	delete(e.eventFilterCache, key)

	// Clear different event destination handlers
	if spec.Destination.HTTP != nil {
		eventHandlerKey := newEventHandlerKey(key, cloudEventHandlerTypeHTTP)
		if eventHandler, found := e.eventHandlersCache[eventHandlerKey]; found {
			eventHandler.CloseHandler()
			delete(e.eventHandlersCache, eventHandlerKey)
		}
	}

	if spec.Destination.AzureEventGridTopic != nil {
		eventHandlerKey := newEventHandlerKey(key, cloudEventHandlerTypeAzureEventGridTopic)
		if eventHandler, found := e.eventHandlersCache[eventHandlerKey]; found {
			eventHandler.CloseHandler()
			delete(e.eventHandlersCache, eventHandlerKey)
		}
	}
}

// checkIfEventHandlersExist will check if the event handlers that were created by passing CloudEventSource exist
func (e *EventEmitter) checkIfEventHandlersExist(cloudEventSource eventingv1alpha1.CloudEventSourceInterface) bool {
	e.eventHandlersCacheLock.RLock()
	defer e.eventHandlersCacheLock.RUnlock()

	key := cloudEventSource.GenerateIdentifier()

	for k := range e.eventHandlersCache {
		if strings.Contains(k, key) {
			return true
		}
	}
	return false
}

// startEventLoop begins a loop that processes CloudEvents and emits them to the appropriate handlers
func (e *EventEmitter) startEventLoop(ctx context.Context, cloudEventSourceI eventingv1alpha1.CloudEventSourceInterface, cloudEventSourceMutex sync.Locker) {
	e.log.V(1).Info("Starting CloudEventSource event loop", "name", cloudEventSourceI.GetName())

	namespace := cloudEventSourceI.GetNamespace()

	for {
		select {
		case eventData := <-e.cloudEventProcessingChan:
			e.log.V(1).Info("Consuming event from CloudEventSource", "name", cloudEventSourceI.GetName())
			e.emitEventByHandler(eventData)
			e.checkEventHandlers(ctx, cloudEventSourceI, cloudEventSourceMutex)
			metricscollector.RecordCloudEventQueueStatus(namespace, len(e.cloudEventProcessingChan))

		case <-ctx.Done():
			e.log.V(1).Info("CloudEventSource loop has been stopped", "name", cloudEventSourceI.GetName())
			metricscollector.RecordCloudEventQueueStatus(namespace, len(e.cloudEventProcessingChan))
			return
		}
	}
}

// checkEventHandlers will check each eventhandler active status
func (e *EventEmitter) checkEventHandlers(ctx context.Context, cloudEventSourceI eventingv1alpha1.CloudEventSourceInterface, cloudEventSourceMutex sync.Locker) {
	cloudEventSourceMutex.Lock()
	defer cloudEventSourceMutex.Unlock()

	e.log.V(1).Info("Checking event handlers status", "name", cloudEventSourceI.GetName())

	// Get the latest object
	err := e.client.Get(ctx, types.NamespacedName{
		Name:      cloudEventSourceI.GetName(),
		Namespace: cloudEventSourceI.GetNamespace(),
	}, cloudEventSourceI)

	if err != nil {
		e.log.Error(err, "Error getting cloudEventSource", "cloudEventSource", cloudEventSourceI.GetName())
		return
	}

	keyPrefix := cloudEventSourceI.GenerateIdentifier()
	needUpdate := false
	cloudEventSourceStatus := cloudEventSourceI.GetStatus().DeepCopy()

	e.eventHandlersCacheLock.RLock()

	for k, v := range e.eventHandlersCache {
		if strings.Contains(k, keyPrefix) {
			e.log.V(1).Info("Checking event handler status",
				"handler", k,
				"current-status", cloudEventSourceI.GetStatus().Conditions.GetActiveCondition().Status)

			if v.GetActiveStatus() != cloudEventSourceI.GetStatus().Conditions.GetActiveCondition().Status {
				needUpdate = true
				cloudEventSourceStatus.Conditions.SetActiveCondition(
					metav1.ConditionFalse,
					eventingv1alpha1.CloudEventSourceConditionFailedReason,
					eventingv1alpha1.CloudEventSourceConditionFailedMessage,
				)
			}
		}
	}

	e.eventHandlersCacheLock.RUnlock()

	if needUpdate {
		if updateErr := e.updateCloudEventSourceStatus(ctx, cloudEventSourceI, cloudEventSourceStatus); updateErr != nil {
			e.log.Error(updateErr, "Failed to update CloudEventSource status")
		}
	}
}

// Emit is emitting event to both local kubernetes and custom CloudEventSource handler. After emit event to local kubernetes, event will inqueue and waitng for handler's consuming.
func (e *EventEmitter) Emit(object runtime.Object, namespace string, eventType string, cloudeventType eventingv1alpha1.CloudEventType, reason, message string) {
	e.recorder.Event(object, eventType, reason, message)

	e.eventHandlersCacheLock.RLock()
	handlerCount := len(e.eventHandlersCache)
	e.eventHandlersCacheLock.RUnlock()

	if handlerCount == 0 {
		return
	}

	accessor := meta.NewAccessor()
	objectName, _ := accessor.Name(object)
	objectType, _ := accessor.Kind(object)

	eventData := eventdata.EventData{
		Namespace:      namespace,
		CloudEventType: cloudeventType,
		ObjectName:     strings.ToLower(objectName),
		ObjectType:     strings.ToLower(objectType),
		Reason:         reason,
		Message:        message,
		Time:           time.Now().UTC(),
	}

	go e.enqueueEventData(eventData)
}

// enqueueEventData places an event in the processing queue with a timeout
func (e *EventEmitter) enqueueEventData(eventData eventdata.EventData) {
	metricscollector.RecordCloudEventQueueStatus(eventData.Namespace, len(e.cloudEventProcessingChan))

	select {
	case e.cloudEventProcessingChan <- eventData:
		e.log.V(1).Info("Event enqueued successfully")
	case <-time.After(maxWaitingEnqueueTime):
		e.log.Error(nil, "Failed to enqueue CloudEvent - queue might be full or blocked")
	}
}

// emitEventByHandler handles event emitting. It will follow these logic:
// 1. If there is a new EventData, call all handlers for emitting.
// 2. Once there is an error when emitting event, record the handler's key and reqeueu this EventData.
// 3. If the maximum number of retries has been exceeded, discard this event.
func (e *EventEmitter) emitEventByHandler(eventData eventdata.EventData) {
	if eventData.RetryTimes >= maxRetryTimes {
		e.log.Error(eventData.Err,
			"Failed to emit event after maximum retry attempts - dropping event",
			"handlerKey", eventData.HandlerKey,
			"retryCount", eventData.RetryTimes,
			"objectName", eventData.ObjectName)

		e.eventHandlersCacheLock.RLock()
		handler, found := e.eventHandlersCache[eventData.HandlerKey]
		e.eventHandlersCacheLock.RUnlock()

		if found {
			handler.SetActiveStatus(metav1.ConditionFalse)
		}
		return
	}

	if eventData.HandlerKey == "" {
		e.dispatchToAllHandlers(eventData)
	} else {
		e.retrySpecificHandler(eventData)
	}
}

// dispatchToAllHandlers sends an event to all registered handlers
func (e *EventEmitter) dispatchToAllHandlers(eventData eventdata.EventData) {
	e.eventHandlersCacheLock.RLock()
	defer e.eventHandlersCacheLock.RUnlock()

	for key, handler := range e.eventHandlersCache {
		// Check if the event is filtered
		e.eventFilterCacheLock.RLock()
		identifierKey := getPrefixIdentifierFromKey(key)
		filter := e.eventFilterCache[identifierKey]

		isFiltered := false
		if filter != nil {
			isFiltered = filter.FilterEvent(eventData.CloudEventType)
			if isFiltered {
				e.log.V(1).Info("Event is filtered out by configuration",
					"cloudEventType", eventData.CloudEventType,
					"eventIdentifier", identifierKey)
			}
		}
		e.eventFilterCacheLock.RUnlock()

		if isFiltered {
			continue
		}

		// Create a copy of the event data for this handler
		handlerEventData := eventData
		handlerEventData.HandlerKey = key

		if handler.GetActiveStatus() == metav1.ConditionTrue {
			go handler.EmitEvent(handlerEventData, e.emitErrorHandle)

			metricscollector.RecordCloudEventEmitted(
				eventData.Namespace,
				getSourceNameFromKey(key),
				getHandlerTypeFromKey(key),
			)
		} else {
			e.log.V(1).Info("Event handler is not active - skipping",
				"handler", key,
				"objectName", eventData.ObjectName)
		}
	}
}

// retrySpecificHandler retries sending an event to a specific handler
func (e *EventEmitter) retrySpecificHandler(eventData eventdata.EventData) {
	e.log.Info("Retrying event emission",
		"handler", eventData.HandlerKey,
		"retry", fmt.Sprintf("%d/%d", eventData.RetryTimes, maxRetryTimes),
		"error", eventData.Err)

	e.eventHandlersCacheLock.RLock()
	handler, found := e.eventHandlersCache[eventData.HandlerKey]
	e.eventHandlersCacheLock.RUnlock()

	if found && handler.GetActiveStatus() == metav1.ConditionTrue {
		go handler.EmitEvent(eventData, e.emitErrorHandle)
	}
}

// emitErrorHandle handles errors that occur when emitting events
func (e *EventEmitter) emitErrorHandle(eventData eventdata.EventData, err error) {
	metricscollector.RecordCloudEventEmittedError(
		eventData.Namespace,
		getSourceNameFromKey(eventData.HandlerKey),
		getHandlerTypeFromKey(eventData.HandlerKey),
	)

	if eventData.RetryTimes >= maxRetryTimes {
		e.log.V(1).Info("Maximum retry count exceeded - setting handler to failure status",
			"handler", eventData.HandlerKey,
			"retryCount", eventData.RetryTimes)

		e.eventHandlersCacheLock.RLock()
		handler, found := e.eventHandlersCache[eventData.HandlerKey]
		e.eventHandlersCacheLock.RUnlock()

		if found {
			handler.SetActiveStatus(metav1.ConditionFalse)
		}
		return
	}

	// Prepare event data for retry
	requeueData := eventData
	requeueData.HandlerKey = eventData.HandlerKey
	requeueData.RetryTimes++
	requeueData.Err = err

	e.enqueueEventData(requeueData)
}

// setCloudEventSourceStatusActive sets the status of a CloudEventSource to active
func (e *EventEmitter) setCloudEventSourceStatusActive(ctx context.Context, cloudEventSourceI eventingv1alpha1.CloudEventSourceInterface) error {
	cloudEventSourceStatus := cloudEventSourceI.GetStatus()
	cloudEventSourceStatus.Conditions.SetActiveCondition(
		metav1.ConditionTrue,
		eventingv1alpha1.CloudEventSourceConditionActiveReason,
		eventingv1alpha1.CloudEventSourceConditionActiveMessage,
	)
	return e.updateCloudEventSourceStatus(ctx, cloudEventSourceI, cloudEventSourceStatus)
}

// updateCloudEventSourceStatus updates the status of a CloudEventSource
func (e *EventEmitter) updateCloudEventSourceStatus(ctx context.Context, cloudEventSourceI eventingv1alpha1.CloudEventSourceInterface, cloudEventSourceStatus *eventingv1alpha1.CloudEventSourceStatus) error {
	e.log.V(1).Info("Updating CloudEventSource status", "name", cloudEventSourceI.GetName())

	transform := func(runtimeObj client.Object, target interface{}) error {
		status, ok := target.(eventingv1alpha1.CloudEventSourceStatus)
		if !ok {
			return fmt.Errorf("transform target is not eventingv1alpha1.CloudEventSourceStatus type: %v", target)
		}

		switch obj := runtimeObj.(type) {
		case *eventingv1alpha1.CloudEventSource:
			e.log.V(1).Info("Updating CloudEventSource status", "status", status)
			obj.Status = status
		case *eventingv1alpha1.ClusterCloudEventSource:
			e.log.V(1).Info("Updating ClusterCloudEventSource status", "status", status)
			obj.Status = status
		default:
			return fmt.Errorf("unsupported CloudEventSource type: %T", runtimeObj)
		}

		return nil
	}

	if err := kedastatus.TransformObject(ctx, e.client, e.log, cloudEventSourceI, *cloudEventSourceStatus, transform); err != nil {
		e.log.Error(err, "Failed to update CloudEventSource status")
		return fmt.Errorf("failed to update CloudEventSource status: %w", err)
	}

	return nil
}

// newEventHandlerKey generates a unique key for an event handler
func newEventHandlerKey(kindNamespaceName string, handlerType string) string {
	return fmt.Sprintf("%s.%s", kindNamespaceName, handlerType)
}

// getPrefixIdentifierFromKey extracts the prefix identifier (CloudEventSource.Namespace.Name) from a handler key
func getPrefixIdentifierFromKey(handlerKey string) string {
	parts := strings.Split(handlerKey, ".")
	if len(parts) >= 3 {
		return strings.Join(parts[:3], ".")
	}
	return ""
}

// getHandlerTypeFromKey extracts the handler type from a handler key
func getHandlerTypeFromKey(handlerKey string) string {
	parts := strings.Split(handlerKey, ".")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

// getSourceNameFromKey extracts the source name from a handler key
func getSourceNameFromKey(handlerKey string) string {
	parts := strings.Split(handlerKey, ".")
	if len(parts) >= 3 {
		return parts[2]
	}
	return ""
}
