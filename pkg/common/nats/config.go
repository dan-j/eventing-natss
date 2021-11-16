package nats

import (
	"fmt"
	"k8s.io/apimachinery/pkg/util/yaml"
	commonconfig "knative.dev/eventing-natss/pkg/common/config"
	"knative.dev/eventing-natss/pkg/common/constants"
)

func LoadEventingNatsConfig(configMap map[string]string) (config commonconfig.EventingNatsConfig, err error) {
	eventingNats, ok := configMap[constants.EventingNatsSettingsConfigKey]
	if !ok {
		return config, fmt.Errorf("missing configmap entry: %s", constants.EventingNatsSettingsConfigKey)
	}

	return config, yaml.Unmarshal([]byte(eventingNats), &config)
}
