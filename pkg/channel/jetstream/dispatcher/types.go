package dispatcher

import (
	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"

	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/channel/fanout"
)

type StreamNameFunc func(nc *v1alpha1.NatsJetStreamChannel) string
type SubjectFunc func(namespace, name string) string
type ConsumerNameFunc func(subID string) string

// EnqueueFunc is passed to the Reconciler for when a follower instance attempts to sync on a Consumer which does not
// yet exist
type EnqueueFunc func(ref types.NamespacedName)

type NatsDispatcherArgs struct {
	JetStream nats.JetStreamContext

	SubjectFunc      SubjectFunc
	ConsumerNameFunc ConsumerNameFunc

	PodName       string
	ContainerName string
}

type ChannelConfig struct {
	channel.ChannelReference
	HostName               string
	StreamName             string
	ConsumerConfigTemplate *v1alpha1.ConsumerConfigTemplate
	Subscriptions          []Subscription
}

func (cc ChannelConfig) SubscriptionsUIDs() []string {
	res := make([]string, 0, len(cc.Subscriptions))
	for _, s := range cc.Subscriptions {
		res = append(res, string(s.UID))
	}
	return res
}

type Subscription struct {
	UID types.UID
	fanout.Subscription
}

type envConfig struct {
	PodName       string `envconfig:"POD_NAME" required:"true"`
	ContainerName string `envconfig:"CONTAINER_NAME" required:"true"`
}

type SubscriberStatusType int

const (
	SubscriberStatusTypeCreated SubscriberStatusType = iota
	SubscriberStatusTypeSkipped
	SubscriberStatusTypeUpToDate
	SubscriberStatusTypeError
	SubscriberStatusTypeDeleted
)

type SubscriberStatus struct {
	UID   types.UID
	Type  SubscriberStatusType
	Error error
}

func NewSubscriberStatusCreated(uid types.UID) SubscriberStatus {
	return SubscriberStatus{UID: uid, Type: SubscriberStatusTypeCreated}
}

func NewSubscriberStatusUpToDate(uid types.UID) SubscriberStatus {
	return SubscriberStatus{UID: uid, Type: SubscriberStatusTypeUpToDate}
}

func NewSubscriberStatusSkipped(uid types.UID) SubscriberStatus {
	return SubscriberStatus{UID: uid, Type: SubscriberStatusTypeSkipped}
}

func NewSubscriberStatusError(uid types.UID, err error) SubscriberStatus {
	return SubscriberStatus{UID: uid, Type: SubscriberStatusTypeError, Error: err}
}
