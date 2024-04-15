package internal

import "github.com/cloudevents/sdk-go/v2/binding"

// WithoutFinish returns a new message that does not call the original message's Finish method.
// This is to work around the fact that the kncloudevents dispatcher always calls Finish with a nil
// error, even if an error occurred.
func WithoutFinish(msg binding.Message) binding.Message {
	return &withoutFinish{msg}
}

type withoutFinish struct {
	binding.Message
}

func (m *withoutFinish) Finish(error) error {
	return nil
}
