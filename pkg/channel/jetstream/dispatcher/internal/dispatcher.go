package internal

import (
	"context"
	"net/http"

	"github.com/cloudevents/sdk-go/v2/protocol"
	"go.uber.org/multierr"
	"knative.dev/eventing/pkg/kncloudevents"
	v1 "knative.dev/pkg/apis/duck/v1"
)

// ReportWithoutRetry disables retries but injects a delay nak into result if a retry would have
// been triggered.
func ReportWithoutRetry(result *error, attempt int, cfg *kncloudevents.RetryConfig) *kncloudevents.RetryConfig {
	if cfg == nil {
		return nil
	}

	next := *cfg

	next.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		retry, err := cfg.CheckRetry(ctx, resp, err)

		if retry {
			if cfg.Backoff != nil {
				multierr.AppendInto(result, NewDelayNak(cfg.Backoff(attempt, resp)))
			} else {
				multierr.AppendInto(result, protocol.ResultNACK)
			}
		}

		return false, err
	}

	return &next
}

func WithRetryReporter(result *error, attempt int, cfg *kncloudevents.RetryConfig) kncloudevents.SendOption {
	return kncloudevents.WithRetryConfig(ReportWithoutRetry(result, attempt, cfg))
}

func WithDeadLetterOnLastAttempt(attempt int, retryCfg *kncloudevents.RetryConfig, deadLetter *v1.Addressable) kncloudevents.SendOption {
	if isLastAttempt(attempt, retryCfg) {
		return kncloudevents.WithDeadLetterSink(deadLetter)
	}

	return kncloudevents.WithDeadLetterSink(nil)
}

func isLastAttempt(attempt int, retryCfg *kncloudevents.RetryConfig) bool {
	return retryCfg == nil || attempt >= retryCfg.RetryMax
}
