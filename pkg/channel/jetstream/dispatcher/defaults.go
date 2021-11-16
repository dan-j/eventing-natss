package dispatcher

import (
	"github.com/nats-io/nats.go"
	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
	"knative.dev/eventing-natss/pkg/channel/jetstream/utils"
)

func buildStreamConfig(streamName, subject string, config *v1alpha1.StreamConfig) *nats.StreamConfig {
	if config == nil {
		return &nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{subject},
		}
	}

	subjects := make([]string, 0, len(config.AdditionalSubjects)+1)
	subjects = append(subjects, subject)
	subjects = append(subjects, config.AdditionalSubjects...)

	return &nats.StreamConfig{
		Name:         streamName,
		Subjects:     subjects,
		Retention:    utils.ConvertRetentionPolicy(config.Retention, nats.LimitsPolicy),
		MaxConsumers: config.MaxConsumers,
		MaxMsgs:      config.MaxMsgs,
		MaxBytes:     config.MaxBytes,
		Discard:      utils.ConvertDiscardPolicy(config.Discard, nats.DiscardOld),
		MaxAge:       config.MaxAge.Duration,
		MaxMsgSize:   config.MaxMsgSize,
		Storage:      utils.ConvertStorage(config.Storage, nats.FileStorage),
		Replicas:     config.Replicas,
		NoAck:        config.NoAck,
		Duplicates:   config.DuplicateWindow.Duration,
		Placement:    utils.ConvertPlacement(config.Placement),
		Mirror:       utils.ConvertStreamSource(config.Mirror),
		Sources:      utils.ConvertStreamSources(config.Sources),
	}

}

func buildConsumerConfig(consumerName string, template *v1alpha1.ConsumerConfigTemplate) *nats.ConsumerConfig {
	consumerConfig := nats.ConsumerConfig{
		Durable:        consumerName,
		DeliverGroup:   consumerName,
		DeliverSubject: nats.NewInbox(),
		AckPolicy:      nats.AckExplicitPolicy,
	}

	if template != nil {
		consumerConfig.DeliverPolicy = utils.ConvertDeliverPolicy(template.DeliverPolicy, nats.DeliverAllPolicy)
		consumerConfig.OptStartSeq = template.OptStartSeq
		consumerConfig.AckWait = template.AckWait.Duration
		consumerConfig.MaxDeliver = template.MaxDeliver
		consumerConfig.FilterSubject = template.FilterSubject
		consumerConfig.ReplayPolicy = utils.ConvertReplayPolicy(template.ReplayPolicy, nats.ReplayInstantPolicy)
		consumerConfig.RateLimit = template.RateLimitBPS
		consumerConfig.SampleFrequency = template.SampleFrequency
		consumerConfig.MaxAckPending = template.MaxAckPending

		if template.OptStartTime != nil {
			consumerConfig.OptStartTime = &template.OptStartTime.Time
		}
	}

	return &consumerConfig
}
