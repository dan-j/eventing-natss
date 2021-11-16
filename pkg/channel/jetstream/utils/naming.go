package utils

import (
	"fmt"
	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
	"strings"
)

var (
	streamNameReplacer = strings.NewReplacer("-", "_")
	uidReplacer        = strings.NewReplacer("-", "")
)

// StreamName builds a valid NATS Jetstream stream name from a *v1alpha1.NatsJetStreamChannel.
// If overrideName is not set, the format is NAMESPACE__NAME, where hyphens in the namespace or name are replaced
// with _.
//
// For example:
// - "default/channel" => "DEFAULT__CHANNEL"
// - "knative-eventing/my-channel" => "KNATIVE_EVENTING__MY_CHANNEL"
func StreamName(nc *v1alpha1.NatsJetStreamChannel) string {
	if nc.Spec.Stream.OverrideName != "" {
		return nc.Spec.Stream.OverrideName
	}

	return strings.ToUpper(streamNameReplacer.Replace(fmt.Sprintf("%s__%s", nc.Namespace, nc.Name)))
}

func QueueName(streamName string) string {
	return fmt.Sprintf("%s._knative", streamName)
}

func SubjectName(namespace, name string) string {
	return fmt.Sprintf("%s.%s._knative", namespace, name)
}

func ConsumerName(subUID string) string {
	return fmt.Sprintf("KN_SUB_%s", strings.ToUpper(uidReplacer.Replace(subUID)))
}
