/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"sync"

	cejs "github.com/cloudevents/sdk-go/protocol/nats_jetstream/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/controller"
	pkgreconciler "knative.dev/pkg/reconciler"

	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
	commonce "knative.dev/eventing-natss/pkg/common/cloudevents"
	"knative.dev/eventing-natss/pkg/tracing"
	tracingnats "knative.dev/eventing-natss/pkg/tracing/nats"

	"knative.dev/eventing/pkg/auth"
	eventingchannels "knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/eventingtls"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
)

var (
	ErrConsumerNotFound = errors.New("consumer not found")
)

// Dispatcher is responsible for managing both directions of events over the NatsJetStreamChannel. It manages the
// lifecycle of the following components:
// - Stream per NatsJetStreamChannel
// - HTTP receiver which publishes to the desired Stream
// - Consumer per .spec.subscribers[] of a channel, forwarding events to the specified subscriber address.
type Dispatcher struct {
	receiver   *eventingchannels.EventReceiver
	dispatcher *kncloudevents.Dispatcher
	reporter   eventingchannels.StatsReporter
	recorder   record.EventRecorder

	js jetstream.JetStream

	publishSubjectFunc StreamSubjectFunc
	consumerNameFunc   ConsumerNameFunc

	// hostToChannelMap represents a map[string]eventingchannels.ChannelReference
	hostToChannelMap sync.Map

	// channelSubscribers holds a view of all subscription UIDs we're aware of for a given channel
	// and is used to determine whether to create/update/delete a Consumer. Although usually true
	// in the happy-path, it may not directly correlate with the actual Consumers we have currently
	// running, i.e. due to a runtime failure. The consumers map is the source of truth for what
	// consumers are currently running.
	channelSubscribers     map[types.NamespacedName]sets.Set[types.UID]
	channelSubscribersLock sync.Mutex

	// consumers holds the current running consumers, keyed by their Subscription UID they are
	// automatically removed when a consumer is stopped, either due to an error or a deliberate
	// stop/removal.
	consumers map[types.UID]*Consumer

	// consumersLock ensures that the consumers map is accessed safely across goroutines. Because
	// Consumers are removed from the map as soon as the consumer stops, it may occur outside the
	// ReconcileConsumers method goroutine (which is locked by channelSubscribersLock). This means
	// that at any point in time during a reconcile, we may have certain states:
	// - updating a consumer which has just stopped, in which case we need to handle it and recreate
	// - removing a consumer which has just stopped, in which case this should be a no-op
	consumersLock sync.Mutex

	// startCtx is the context used to start the Dispatcher and is propagated down to all consumers
	// so that an exiting dispatcher can stop all consumers.
	startCtx context.Context
}

func NewDispatcher(ctx context.Context, args NatsDispatcherArgs) (*Dispatcher, error) {
	logger := logging.FromContext(ctx)

	reporter := eventingchannels.NewStatsReporter(args.ContainerName, kmeta.ChildName(args.PodName, uuid.New().String()))

	oidcTokenProvider := auth.NewOIDCTokenProvider(ctx)
	d := &Dispatcher{
		dispatcher: kncloudevents.NewDispatcher(eventingtls.ClientConfig{}, oidcTokenProvider),
		reporter:   reporter,
		recorder:   controller.GetEventRecorder(ctx),

		js: args.JetStream,

		publishSubjectFunc: args.SubjectFunc,
		consumerNameFunc:   args.ConsumerNameFunc,

		channelSubscribers: make(map[types.NamespacedName]sets.Set[types.UID]),
		consumers:          make(map[types.UID]*Consumer),
	}

	receiverFunc, err := eventingchannels.NewEventReceiver(
		d.messageReceiver,
		logger.Desugar(),
		reporter,
		eventingchannels.ResolveChannelFromHostHeader(d.getChannelReferenceFromHost),
	)
	if err != nil {
		logger.Error("failed to create message receiver")
		return nil, err
	}

	d.receiver = receiverFunc

	return d, nil
}

func (d *Dispatcher) Start(ctx context.Context) error {
	if d.receiver == nil {
		return fmt.Errorf("message receiver not set")
	}

	d.startCtx = ctx

	return d.receiver.Start(d.startCtx)
}

// RegisterChannelHost registers the NatsDispatcher to accept HTTP events matching the specified HostName
func (d *Dispatcher) RegisterChannelHost(config ChannelConfig) error {
	if old, ok := d.hostToChannelMap.LoadOrStore(config.HostName, config.ChannelReference); ok {
		// map already contained a channel reference for this hostname, check they both reference the same channel,
		// we only care about the Name/Namespace pair matching, since stream name is immutable.
		if old != config.ChannelReference {
			// if something is already there, but it's not the same channel, then fail
			return fmt.Errorf(
				"duplicate hostName found. Each channel must have a unique host header. HostName:%s, channel:%s.%s, channel:%s.%s",
				config.HostName,
				old.(eventingchannels.ChannelReference).Namespace,
				old.(eventingchannels.ChannelReference).Name,
				config.Namespace,
				config.Name,
			)
		}
	}

	return nil
}

func (d *Dispatcher) ReconcileConsumers(ctx context.Context, config ChannelConfig, isLeader bool) pkgreconciler.Event {
	logger := logging.FromContext(ctx).With(zap.Bool("is_leader", isLeader))
	channelNamespacedName := types.NamespacedName{
		Namespace: config.Namespace,
		Name:      config.Name,
	}

	d.channelSubscribersLock.Lock()
	defer d.channelSubscribersLock.Unlock()

	logger.Debug("reconciling consumers")

	currentSubs, ok := d.channelSubscribers[channelNamespacedName]
	if !ok {
		currentSubs = sets.New[types.UID]()
	}
	expectedSubs := sets.New[types.UID](config.SubscriptionsUIDs()...)

	toAddSubs := expectedSubs.Difference(currentSubs)
	toRemoveSubs := currentSubs.Difference(expectedSubs)
	toUpdateSubs := currentSubs.Intersection(expectedSubs)

	nextSubs := sets.New[types.UID]()

	defer func() {
		d.channelSubscribers[channelNamespacedName] = nextSubs
	}()

	// err may be a single error, or wrap multiple errors via the multierr package.
	// if this is non-nil at the end, we return a warning event with the error message(s).
	var err error

	// TODO: what if orphaned consumers are left on the JetStream server? for example if a
	//  subscription is deleted but the consumer deletion fails.
	//  - could we add a finalizer to the consumer to ensure it is deleted when the subscription is
	//    deleted since this would keep reconciling until it's sorted.
	//  - do we list all consumers of this stream, and use consumerInfo.Config.Metadata to track
	//    if we own it or not? This way any orphaned consumers can be cleaned up at the end of this
	//    method.

	for _, sub := range config.Subscriptions {
		uid := sub.UID
		nextSubs.Insert(uid)
		subLogger := logger.With(zap.String("sub_uid", string(uid)))
		ctx := logging.WithLogger(ctx, subLogger)

		var status SubscriberStatusType

		if toUpdateSubs.Has(uid) {
			subLogger.Debugw("updating existing subscription")

			if subErr := d.updateSubscription(ctx, config, sub, isLeader); subErr != nil {
				if errors.Is(subErr, ErrConsumerNotFound) {
					subLogger.Debugw("existing subscription not found, will be re-subscribed")
					toAddSubs.Insert(uid)
				} else {
					err = multierr.Append(err, fmt.Errorf("sub_uid(%s): %w", uid, subErr))

					continue
				}
			}

			status = SubscriberStatusTypeUpToDate
		}

		if toAddSubs.Has(uid) {
			subLogger.Debugw("subscription not configured for dispatcher, subscribing")
			var subErr error
			status, subErr = d.subscribe(ctx, config, sub, isLeader)
			if subErr != nil {
				subLogger.Errorw("failed to subscribe", zap.Error(subErr))
				nextSubs.Delete(uid)

				err = multierr.Append(err, fmt.Errorf("sub_uid(%s): %w", uid, subErr))
			}

			if status == SubscriberStatusTypeSkipped {
				// we delete on an error above, so we only need to delete here if we skipped
				nextSubs.Delete(uid)
			}
		}

		logger.Debugw("Subscription status after add/update", zap.String("sub_status", status.String()))
	}

	for _, toRemove := range toRemoveSubs.UnsortedList() {
		subLogger := logger.With(zap.String("sub_uid", string(toRemove)))
		subLogger.Debugw("extraneous subscription configured for dispatcher, unsubscribing")

		multierr.AppendFunc(&err, func() error {
			if err := d.unsubscribe(ctx, config, toRemove, isLeader); err != nil {
				return fmt.Errorf("sub_uid(%s): %w", toRemove, err)
			}

			return nil
		})

	}

	if err != nil {
		return pkgreconciler.NewEvent(
			corev1.EventTypeWarning,
			ReasonJetstreamConsumerFailed,
			"failed to reconcile JetStream consumers with errors:\n\t%s",
			err.Error(),
		)
	}

	return nil
}

func (d *Dispatcher) updateSubscription(ctx context.Context, config ChannelConfig, sub Subscription, isLeader bool) (err error) {
	defer func() {
		if err != nil {
			// if we fail to update the subscription, we should stop *Consumer. In the case err
			// is ErrConsumerNotFound, we will end up creating/updating it via subscribe(). If it's
			// any other error, the error will be added as an event to the channel and the consumer
			// will be skipped until the next reconcile.
			d.stopConsumer(config.Object, sub.UID)
		}
	}()

	d.consumersLock.Lock()
	defer d.consumersLock.Unlock()

	consumer, ok := d.consumers[sub.UID]
	if !ok {
		return ErrConsumerNotFound
	}

	consumer.UpdateSubscription(sub)

	logger := logging.FromContext(ctx)

	if isLeader {
		_, err := d.js.UpdateConsumer(
			ctx,
			config.StreamName,
			buildConsumerConfig(d.consumerNameFunc(string(sub.UID)), config.ConsumerConfigTemplate, sub.RetryConfig),
		)
		if err != nil {
			logger.Errorw("failed to update queue subscription for consumer", zap.Error(err))

			if errors.Is(err, jetstream.ErrConsumerNotFound) {
				return ErrConsumerNotFound
			}

			return err
		}
	}

	return nil
}

func (d *Dispatcher) subscribe(ctx context.Context, config ChannelConfig, sub Subscription, isLeader bool) (SubscriberStatusType, error) {
	logger := logging.FromContext(ctx)

	jsConsumer, err := d.getOrEnsureConsumer(ctx, config, sub, isLeader)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			//	this error can only occur if the dispatcher is not the leader
			logger.Infow("dispatcher not leader and consumer does not exist yet")
			return SubscriberStatusTypeSkipped, nil
		}

		logger.Errorw("failed to getOrEnsureConsumer during subscribe")
		return SubscriberStatusTypeError, err
	}

	consumer, err := NewConsumer(ctx, jsConsumer, sub, d.dispatcher, d.reporter, config.Namespace)
	if err != nil {
		logger.Errorw("failed to create consumer", zap.Error(err))
		return SubscriberStatusTypeError, err
	}

	d.startConsumer(consumer, sub, config.Object)

	return SubscriberStatusTypeCreated, nil
}

func (d *Dispatcher) unsubscribe(ctx context.Context, config ChannelConfig, uid types.UID, isLeader bool) (err error) {
	d.stopConsumer(config.Object, uid)

	if !isLeader {
		return nil
	}

	if err := d.deleteJetStreamConsumer(ctx, config, string(uid)); err != nil {
		// TODO: see comment in ReconcileConsumers on what to do if this fails
		return fmt.Errorf("failed to delete consumer from JetStream server: %w", err)
	}

	return nil
}

// getOrEnsureConsumer obtains the current ConsumerInfo for this Subscription, updating/creating one if the dispatcher
// is a leader.
func (d *Dispatcher) getOrEnsureConsumer(ctx context.Context, config ChannelConfig, sub Subscription, isLeader bool) (jetstream.Consumer, error) {
	logger := logging.FromContext(ctx)

	consumerName := d.consumerNameFunc(string(sub.UID))

	l := d.js.StreamNames(ctx)
	for name := range l.Name() {
		logger.Debugw("found stream", zap.String("stream", name))
	}

	if isLeader {
		consumer, err := d.js.CreateOrUpdateConsumer(
			ctx,
			config.StreamName,
			buildConsumerConfig(consumerName, config.ConsumerConfigTemplate, sub.RetryConfig),
		)
		if err != nil {
			logger.Errorw("failed to create consumer", zap.Error(err))

			return nil, err
		}

		return consumer, nil
	}

	// dispatcher isn't leader, try and retrieve an existing consumer
	return d.js.Consumer(ctx, config.StreamName, consumerName)
}

func (d *Dispatcher) deleteJetStreamConsumer(ctx context.Context, config ChannelConfig, uid string) error {
	logger := logging.FromContext(ctx)
	consumerName := d.consumerNameFunc(uid)

	if err := d.js.DeleteConsumer(ctx, config.StreamName, consumerName); err != nil {
		logger.Errorw("failed to delete JetStream Consumer", zap.Error(err))
		return err
	}

	return nil
}

func (d *Dispatcher) messageReceiver(ctx context.Context, ch eventingchannels.ChannelReference, event event.Event, _ nethttp.Header) error {
	message := binding.ToMessage(&event)

	logger := logging.FromContext(ctx)
	logger.Debugw("received message from HTTP receiver")

	eventID := commonce.IDExtractorTransformer("")

	// TODO: discuss this with someone who knows about tracing rules for CloudEvents because it
	//  confuses me.
	//
	// Based on the link below, I think the best case is it should be the client's responsibility to
	// set the trace extensions on the event, we should only be adding them here if they don't
	// already exist (which isn't what tracing.SerializeTraceTransformers does)
	//
	// https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/distributed-tracing.md#using-the-distributed-tracing-extension
	transformers := append([]binding.Transformer{&eventID},
		tracing.SerializeTraceTransformers(trace.FromContext(ctx).SpanContext())...,
	)

	writer := new(bytes.Buffer)
	if err := cejs.WriteMsg(ctx, message, writer, transformers...); err != nil {
		logger.Error("failed to write binding.Message to bytes.Buffer")
		return err
	}

	subject := d.publishSubjectFunc(ch.Namespace, ch.Name)
	logger = logger.With(zap.String("msg_id", string(eventID)))
	logger.Debugw("parsed message into JetStream encoding, publishing to subject", zap.String("subj", subject))

	msg := nats.NewMsg(subject)
	msg.Data = writer.Bytes()

	ctx, span := trace.StartSpan(ctx, "nats.publish", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	tracingnats.InjectSpanContext(msg, span.SpanContext())

	// publish the message, passing the cloudevents ID as the MsgId so that we can benefit from detecting duplicate
	// messages
	_, err := d.js.PublishMsg(ctx, msg, jetstream.WithMsgID(string(eventID)))
	if err != nil {
		logger.Errorw("failed to publish message", zap.Error(err))
		return err
	}

	logger.Debugw("successfully published message to JetStream")
	return nil
}

func (d *Dispatcher) getChannelReferenceFromHost(host string) (eventingchannels.ChannelReference, error) {
	cr, ok := d.hostToChannelMap.Load(host)
	if !ok {
		return eventingchannels.ChannelReference{}, eventingchannels.UnknownHostError(host)
	}
	return cr.(eventingchannels.ChannelReference), nil
}

// startConsumer updates the consumers map and starts the consumer in a new goroutine. Upon exit of
// the consumer goroutine, the consumer is removed from the consumers map.
func (d *Dispatcher) startConsumer(consumer *Consumer, sub Subscription, channel *v1alpha1.NatsJetStreamChannel) {
	d.consumersLock.Lock()
	defer d.consumersLock.Unlock()

	uid := sub.UID

	if _, ok := d.consumers[uid]; ok {
		// panic over an error because the code should be written to prevent this, if this happens
		// then we have a memory leak.
		panic(fmt.Errorf("cannot start consumer, consumer already exists for UID: %s", uid))
	}

	d.consumers[uid] = consumer

	go func() {
		defer func() {
			d.consumersLock.Lock()
			defer d.consumersLock.Unlock()

			delete(d.consumers, uid)
		}()

		if err := consumer.Start(); err != nil {
			// TODO: should this trigger a resync of d.channelSubscribers for this channel? If so it
			//  needs to be at the ReconcileConsumers level so that the channelSubscribersLock is
			//  held properly
			d.recorder.Eventf(channel, corev1.EventTypeWarning, ReasonJetstreamConsumerFailed, "consumer start failure: %s", err.Error())
		}
	}()
}

// stopConsumer calls Close on the consumer whilst properly maintaining the lock on the consumers
// map. This method does not remove the consumer from the map as this is handled by the goroutine
// spawned in startConsumer.
func (d *Dispatcher) stopConsumer(obj *v1alpha1.NatsJetStreamChannel, uid types.UID) {
	d.consumersLock.Lock()
	defer d.consumersLock.Unlock()

	consumer, ok := d.consumers[uid]
	if !ok {
		return
	}

	if err := consumer.Close(); err != nil {
		d.recorder.Eventf(obj, corev1.EventTypeWarning, ReasonJetstreamConsumerFailed, "consumer close failure: %s", err.Error())
	}
}
