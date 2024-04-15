package internal

import (
	"time"

	"github.com/cloudevents/sdk-go/v2/protocol"
)

type delayNak struct {
	delay time.Duration
	err   error
}

func (d *delayNak) Error() string {
	return d.err.Error()
}

func (d *delayNak) Unwrap() error {
	return d.err
}

func (d *delayNak) Delay() time.Duration {
	return d.delay
}

func NewDelayNak(delay time.Duration) error {
	return &delayNak{
		delay: delay,
		err:   protocol.ResultNACK,
	}
}
