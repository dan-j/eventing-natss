package utils

import (
	"github.com/nats-io/nats.go"
	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
)

func ConvertDeliverPolicy(in v1alpha1.DeliverPolicy, def nats.DeliverPolicy) nats.DeliverPolicy {
	switch in {
	case v1alpha1.AllDeliverPolicy:
		return nats.DeliverAllPolicy
	case v1alpha1.LastDeliverPolicy:
		return nats.DeliverLastPolicy
	case v1alpha1.NewDeliverPolicy:
		return nats.DeliverNewPolicy
	case v1alpha1.ByStartSequenceDeliverPolicy:
		return nats.DeliverByStartSequencePolicy
	case v1alpha1.ByStartTimeDeliverPolicy:
		return nats.DeliverByStartTimePolicy
	}

	return def
}

func ConvertReplayPolicy(in v1alpha1.ReplayPolicy, def nats.ReplayPolicy) nats.ReplayPolicy {
	switch in {
	case v1alpha1.InstantReplayPolicy:
		return nats.ReplayInstantPolicy
	case v1alpha1.OriginalReplayPolicy:
		return nats.ReplayOriginalPolicy
	}

	return def
}
