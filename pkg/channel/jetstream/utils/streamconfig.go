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

package utils

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
)

func ConvertRetentionPolicy(in v1alpha1.RetentionPolicy, def jetstream.RetentionPolicy) jetstream.RetentionPolicy {
	switch in {
	case v1alpha1.LimitsRetentionPolicy:
		return jetstream.LimitsPolicy
	case v1alpha1.WorkRetentionPolicy:
		return jetstream.WorkQueuePolicy
	case v1alpha1.InterestRetentionPolicy:
		return jetstream.InterestPolicy
	}

	return def
}

func ConvertDiscardPolicy(in v1alpha1.DiscardPolicy, def jetstream.DiscardPolicy) jetstream.DiscardPolicy {
	switch in {
	case v1alpha1.OldDiscardPolicy:
		return jetstream.DiscardOld
	case v1alpha1.NewDiscardPolicy:
		return jetstream.DiscardNew
	}

	return def
}

func ConvertStorage(in v1alpha1.Storage, def jetstream.StorageType) jetstream.StorageType {
	switch in {
	case v1alpha1.FileStorage:
		return jetstream.FileStorage
	case v1alpha1.MemoryStorage:
		return jetstream.MemoryStorage
	}

	return def
}

func ConvertPlacement(in *v1alpha1.StreamPlacement) *jetstream.Placement {
	if in == nil {
		return nil
	}

	return &jetstream.Placement{
		Cluster: in.Cluster,
		Tags:    in.Tags,
	}
}

func ConvertStreamSource(in *v1alpha1.StreamSource) *jetstream.StreamSource {
	if in == nil {
		return nil
	}

	var startTime *time.Time
	if in.OptStartTime != nil {
		startTime = &in.OptStartTime.Time
	}

	return &jetstream.StreamSource{
		Name:          in.Name,
		OptStartSeq:   in.OptStartSeq,
		OptStartTime:  startTime,
		FilterSubject: in.FilterSubject,
	}
}

func ConvertStreamSources(in []v1alpha1.StreamSource) []*jetstream.StreamSource {
	if in == nil {
		return nil
	}

	arr := make([]*jetstream.StreamSource, len(in))
	for i, source := range in {
		arr[i] = ConvertStreamSource(&source)
	}

	return arr
}
