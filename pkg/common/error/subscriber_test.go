package error

import (
	"errors"
	"fmt"
	"testing"

	"go.uber.org/multierr"
)

func TestSubscriberError_As(t *testing.T) {
	type fields struct {
		otherErr error
		errors   []struct {
			UID string
			Err error
		}
	}
	tests := []struct {
		name   string
		fields fields
		wantAs bool
	}{
		{
			name:   "no errors",
			wantAs: false,
		},
		{
			name: "with one error",
			fields: fields{
				errors: []struct {
					UID string
					Err error
				}{
					{
						UID: "one",
						Err: fmt.Errorf("error one"),
					},
				},
			},
			wantAs: true,
		},
		{
			name: "with multiple errors",
			fields: fields{
				errors: []struct {
					UID string
					Err error
				}{
					{
						UID: "one",
						Err: fmt.Errorf("error one"),
					},
					{
						UID: "two",
						Err: fmt.Errorf("error two"),
					},
				},
			},
			wantAs: true,
		},
		{
			name: "with wrapped sub error",
			fields: fields{
				errors: []struct {
					UID string
					Err error
				}{
					{
						UID: "one",
						Err: fmt.Errorf("error one"),
					},
				},
				otherErr: fmt.Errorf("other error"),
			},
			wantAs: true,
		},
		{
			name: "with other error",
			fields: fields{
				otherErr: fmt.Errorf("other error"),
			},
			wantAs: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// we want to test that our `SubscriberErrors` type can be correctly converted
			// upon being returned by a function returning `error`
			err := func() error {
				var errs SubscriberErrors

				for _, e := range tt.fields.errors {
					errs.AddError(e.UID, e.Err)
				}

				if len(errs) == 0 {
					return nil
				}

				if tt.fields.otherErr != nil {
					return multierr.Append(tt.fields.otherErr, errs)
				}

				return errs
			}()

			var errs SubscriberErrors
			if tt.wantAs != errors.As(err, &errs) {
				t.Errorf("errors.As() unexpected result, want %v", tt.wantAs)
			}
		})
	}
}
