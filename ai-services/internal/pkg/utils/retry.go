package utils

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/project-ai-services/ai-services/internal/pkg/logger"
)

// BackoffFunc type definition.
type BackoffFunc func(currentDelay time.Duration) time.Duration

// nonRetryableError wraps an error to signal Retry that it must not attempt again.
type nonRetryableError struct{ cause error }

func (e *nonRetryableError) Error() string { return e.cause.Error() }
func (e *nonRetryableError) Unwrap() error { return e.cause }

// NonRetryableError wraps err so that Retry stops immediately without further attempts.
func NonRetryableError(err error) error { return &nonRetryableError{cause: err} }

// Retry -> retries based on the retry attempts and initialDelay time set on failure.
// Does exponentialBackOff based on the provided BackoffFunc.
// Set backoff func to nil, if exponentialBackoff is not required.
func Retry(
	ctx context.Context,
	attempts int,
	initialDelay time.Duration,
	backoff BackoffFunc,
	fn func() error,
) error {
	delay := initialDelay
	var err error

	// Run the function initially and if no error do not proceed with retry attempts
	err = fn()
	if err == nil {
		return nil
	}
	// Bail immediately on non-retryable errors.
	var nre *nonRetryableError
	if errors.As(err, &nre) {
		return nre.Unwrap()
	}

	for i := range attempts {
		logger.DebugfCtx(ctx, "\n[Retry] Attempt %d/%d...\n", i+1, attempts)

		if err = fn(); err == nil {
			return nil
		}

		// Bail immediately on non-retryable errors.
		if errors.As(err, &nre) {
			return nre.Unwrap()
		}

		// At Last attempt — stop
		if i == attempts-1 {
			break
		}

		// Sleep till delay
		logger.DebugfCtx(ctx, "[Retry] Sleeping %v before retrying...\n", delay)
		time.Sleep(delay)

		// Apply backoff if provided
		if backoff != nil {
			delay = backoff(delay)
		}
	}

	return fmt.Errorf("retry failed after %d attempts with err: %w", attempts, err)
}
