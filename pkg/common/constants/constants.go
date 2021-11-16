package constants

const (
	// SettingsConfigMapName is the name of the configmap used to hold eventing-nats settings
	SettingsConfigMapName = "config-nats"

	// SettingsConfigMapMountPath is the mount path of the configmap used to hold eventing-nats settings
	SettingsConfigMapMountPath = "/etc/" + SettingsConfigMapName

	// EventingNatsSettingsConfigKey is an entry of the SettingsConfigMapName configmap.
	EventingNatsSettingsConfigKey = "eventing-nats"

	// ConfigMapHashAnnotationKey is an annotation is used by the controller to track updates
	// to config-nats and apply them in the dispatcher deployment
	ConfigMapHashAnnotationKey = "jetstream.eventing.knative.dev/configmap-hash"

	// KnativeConfigMapReaderClusterRole is the cluster role granting workloads access to read configmaps. This is bound
	// to dispatcher serviceaccounts via a rolebinding within the knative-eventing namespace to enable watching of
	// common knative config (i.e. tracing)
	KnativeConfigMapReaderClusterRole = "eventing-config-reader"

	DefaultNatsURL = "nats://nats.nats-io.svc.cluster.local"

	DefaultCredentialFileSecretKey = "nats.creds"
)
