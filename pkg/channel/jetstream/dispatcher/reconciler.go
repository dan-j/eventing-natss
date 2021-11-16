package dispatcher

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	commonerr "knative.dev/eventing-natss/pkg/common/error"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/channel/fanout"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"time"

	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
	jsmreconciler "knative.dev/eventing-natss/pkg/client/injection/reconciler/messaging/v1alpha1/natsjetstreamchannel"
)

const (
	// Name of the corev1.Events emitted from the reconciliation process.

	ReasonJetstreamStreamCreated   = "JetstreamStreamCreated"
	ReasonJetstreamStreamFailed    = "JetstreamStreamFailed"
	ReasonJetstreamConsumerCreated = "JetstreamConsumerCreated"
	ReasonJetstreamConsumerFailed  = "JetstreamConsumerFailed"
)

// Reconciler reconciles incoming NatsJetstreamChannel CRDs by ensuring the following states:
// - Creates a JSM Stream for each channel
// - Creates a HTTP listener which publishes received events to the Stream
// - Creates a consumer for each .spec.subscribers[] and forwards events to the subscriber address
type Reconciler struct {
	js         nats.JetStreamManager
	dispatcher *Dispatcher

	streamNameFunc StreamNameFunc
}

var _ jsmreconciler.Interface = (*Reconciler)(nil)
var _ jsmreconciler.ReadOnlyInterface = (*Reconciler)(nil)
var _ jsmreconciler.Finalizer = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, nc *v1alpha1.NatsJetStreamChannel) pkgreconciler.Event {
	logger := logging.FromContext(ctx)
	logger.Debugw("ReconcileKind for channel", zap.String("channel", nc.Name))

	if err := r.reconcileStream(ctx, nc); err != nil {
		logger.Errorw("failed to reconcile stream", zap.Error(err))
		return err
	}

	return r.syncChannel(ctx, nc, true)
}

func (r *Reconciler) ObserveKind(ctx context.Context, nc *v1alpha1.NatsJetStreamChannel) pkgreconciler.Event {
	logger := logging.FromContext(ctx)
	logger.Debugw("ObserveKind for channel", zap.String("channel", nc.Name))

	return r.syncChannel(ctx, nc, false)
}

func (r *Reconciler) FinalizeKind(ctx context.Context, nc *v1alpha1.NatsJetStreamChannel) pkgreconciler.Event {
	panic("implement me")
}

// reconcileStream ensures that the stream exists with the correct configuration.
func (r *Reconciler) reconcileStream(ctx context.Context, nc *v1alpha1.NatsJetStreamChannel) pkgreconciler.Event {
	logger := logging.FromContext(ctx)

	streamName := r.streamNameFunc(nc)
	primarySubject := r.dispatcher.subjectFunc(nc.Namespace, nc.Name)

	existing, err := r.js.StreamInfo(streamName)
	if err != nil && err != nats.ErrStreamNotFound {
		logger.Errorw("failed to check current stream info")
		return err
	}

	streamConfig := buildStreamConfig(streamName, primarySubject, nc.Spec.Stream.Config)
	isCreating := existing == nil

	// AddStream is idempotent if the config is identical to that on the server
	info, err := r.js.AddStream(streamConfig)
	if err != nil {
		logger.Errorw("failed to add stream")
		controller.GetEventRecorder(ctx).Event(nc, corev1.EventTypeWarning, ReasonJetstreamStreamFailed, err.Error())
		nc.Status.MarkStreamFailed("DispatcherCreateStreamFailed", "Failed to create JetStream stream")
		return err
	}

	if isCreating {
		logger.Infow("jetstream stream created", zap.String("stream_name", info.Config.Name))
		controller.GetEventRecorder(ctx).Event(nc, corev1.EventTypeNormal, ReasonJetstreamStreamCreated, "Dispatcher deployment created")
	}

	nc.Status.MarkStreamTrue()

	return nil
}

func (r *Reconciler) syncChannel(ctx context.Context, nc *v1alpha1.NatsJetStreamChannel, isLeader bool) pkgreconciler.Event {
	logger := logging.FromContext(ctx).With(zap.Bool("is_leader", isLeader))

	if !nc.Status.IsReady() {
		logger.Debugw("NatsJetStreamChannel still not ready, short-circuiting the reconciler", zap.String("channel", nc.Name))
		return nil
	}

	config := r.newConfigFromChannel(nc)

	// Update receiver side
	if err := r.dispatcher.RegisterChannelHost(config); err != nil {
		logger.Errorw("failed to update host to channel map in dispatcher")
		return err
	}

	// Update dispatcher side
	err := r.dispatcher.ReconcileConsumers(logging.WithLogger(ctx, logger), config, isLeader)
	if err != nil {
		logger.Errorw("failure occurred during reconcile consumers", zap.Error(err))

		if errs, ok := err.(commonerr.SubscriberErrors); ok {
			for _, subErr := range errs {
				controller.GetEventRecorder(ctx).Event(
					nc,
					corev1.EventTypeWarning,
					ReasonJetstreamConsumerFailed,
					fmt.Sprintf("Failed to create subscriber for UID: %s: %s", subErr.UID, subErr.Err.Error()),
				)
			}

			// don't let the controller report extra infomation due to subscriber error, just requeue in 10 seconds.
			return controller.NewRequeueAfter(10 * time.Second)
		}

		// if an unexpected err occurred, let the controller decide how
		return err
	}

	return nil
}

// newConfigFromChannel creates a new ChannelConfig from a NatsJetStreamChannel for use in the Dispatcher.
func (r *Reconciler) newConfigFromChannel(nc *v1alpha1.NatsJetStreamChannel) ChannelConfig {
	channelConfig := ChannelConfig{
		ChannelReference: channel.ChannelReference{
			Namespace: nc.Namespace,
			Name:      nc.Name,
		},
		StreamName:             r.streamNameFunc(nc),
		HostName:               nc.Status.Address.URL.Host,
		ConsumerConfigTemplate: nc.Spec.ConsumerConfigTemplate,
	}
	if nc.Spec.SubscribableSpec.Subscribers != nil {
		newSubs := make([]Subscription, len(nc.Spec.SubscribableSpec.Subscribers))
		for i, source := range nc.Spec.SubscribableSpec.Subscribers {
			innerSub, _ := fanout.SubscriberSpecToFanoutConfig(source)

			newSubs[i] = Subscription{
				Subscription: *innerSub,
				UID:          source.UID,
			}
		}
		channelConfig.Subscriptions = newSubs
	}

	return channelConfig
}
