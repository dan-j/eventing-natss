package controller

// envConfig defines the environment variables which can configure the controller
type envConfig struct {
	Image                    string `envconfig:"DISPATCHER_IMAGE" required:"true"`
	DispatcherServiceAccount string `envconfig:"DISPATCHER_SERVICE_ACCOUNT" required:"true"`
}
