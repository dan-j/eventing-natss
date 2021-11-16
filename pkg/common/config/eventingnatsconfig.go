package config

type EventingNatsConfig struct {
	URL  string                 `json:"url,omitempty"`
	Auth *ENAuthConfig          `json:"auth,omitempty"`
	TLS  *EventingNatsConfigTLS `json:"tls,omitempty"`
}

type ENAuthConfig struct {
	CredentialFile *ENCredentialFileConfig `json:"credentialFile,omitempty"`
}

type ENCredentialFileConfig struct {
	Secret *ENSecretConfig `json:"secret"`
}

type ENSecretConfig struct {
	Key        string `json:"key,omitempty"`
	SecretName string `json:"secretName,omitempty"`
}

type EventingNatsConfigTLS struct {
	SecretName string `json:"secretName,omitempty"`
}
