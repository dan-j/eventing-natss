package utils

import (
	"github.com/nats-io/nats.go"
	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
	"time"
)

func ConvertRetentionPolicy(in v1alpha1.RetentionPolicy, def nats.RetentionPolicy) nats.RetentionPolicy {
	switch in {
	case v1alpha1.LimitsRetentionPolicy:
		return nats.LimitsPolicy
	case v1alpha1.WorkRetentionPolicy:
		return nats.WorkQueuePolicy
	case v1alpha1.InterestRetentionPolicy:
		return nats.InterestPolicy
	}

	return def
}

func ConvertDiscardPolicy(in v1alpha1.DiscardPolicy, def nats.DiscardPolicy) nats.DiscardPolicy {
	switch in {
	case v1alpha1.OldDiscardPolicy:
		return nats.DiscardOld
	case v1alpha1.NewDiscardPolicy:
		return nats.DiscardNew
	}

	return def
}

func ConvertStorage(in v1alpha1.Storage, def nats.StorageType) nats.StorageType {
	switch in {
	case v1alpha1.FileStorage:
		return nats.FileStorage
	case v1alpha1.MemoryStorage:
		return nats.MemoryStorage
	}

	return def
}

func ConvertPlacement(in *v1alpha1.StreamPlacement) *nats.Placement {
	if in == nil {
		return nil
	}

	return &nats.Placement{
		Cluster: in.Cluster,
		Tags:    in.Tags,
	}
}

func ConvertStreamSource(in *v1alpha1.StreamSource) *nats.StreamSource {
	if in == nil {
		return nil
	}

	var startTime *time.Time
	if in.OptStartTime != nil {
		startTime = &in.OptStartTime.Time
	}

	return &nats.StreamSource{
		Name:          in.Name,
		OptStartSeq:   in.OptStartSeq,
		OptStartTime:  startTime,
		FilterSubject: in.FilterSubject,
	}
}

func ConvertStreamSources(in []v1alpha1.StreamSource) []*nats.StreamSource {
	if in == nil {
		return nil
	}

	arr := make([]*nats.StreamSource, len(in))
	for i, source := range in {
		arr[i] = ConvertStreamSource(&source)
	}

	return arr
}
