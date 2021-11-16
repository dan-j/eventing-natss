package error

import "fmt"

type SubscriberErrors []SubscriberError

func (e *SubscriberErrors) AddError(uid string, err error) {
	if err == nil {
		return
	}

	if e == nil {
		*e = []SubscriberError{{uid, err}}
	}

	*e = append(*e, SubscriberError{uid, err})
}

func (e SubscriberErrors) Error() string {
	if e == nil || len(e) == 0 {
		return ""
	}

	return fmt.Sprintf("subscriber error occurred %d times", len(e))
}

type SubscriberError struct {
	UID string
	Err error
}

func (e SubscriberError) Error() string {
	return fmt.Sprintf("%s: uid=%s", e.Err, e.UID)
}
