// Copyright (c) 2018 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package retry provides functionality for controlling retries.
//
// Exponential Backoff
//
// Backoff duration after $retryAttempt (first attempt is 0) is defined as:
//
//	backoff =
//	  min(initialBackoff * pow(multiplier, $retryAttempt), maxBackoff == 0 ? +Inf : maxBackoff) *
//	    (1.0 - randomizationFactor + 2 * rand(0, randomizationFactor))
//
// Retrying Failures
//
// Example 1: Opening connection.
//
//	retry.Do(ctx, func() error {
//		return openConnection(&handle)
//	})
//
// Retry Loops
//
// Example 1: Event pulling and dispatching.
//
//	for r := retry.Start(ctx, WithMaxBackoff(200 * time.Millisecond)); r.Next(); {
//		events := pull();
//		if len(events) > 0 {
//			dispatch(events)
//			r.Reset()
//		}
//	}
//	return ctx.Err()
//
// Example 2: Retrying CAS operations.
//
//	for r := retry.Start(ctx); r.Next(); {
//		success, err := kv.CompareAndSwap(key, value)
//		switch {
//		case err != nil:
//			return err
//		case success:
//			return nil
//		default:
//			continue
//		}
//	}
//	return ctx.Err()
//
// Example 3: Waiting for status.
//
//	for r := retry.Start(ctx); r.Next(); {
//		if serverStatus() == StatusRunning {
//			return true
//		}
//	}
//	return false
//
package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// Do retries action until action returns nil, context is done or max attempts limit is reached.
//
// Returns nil if action eventually succeeded, otherwise returns last action error or ctx.Err()
// if action was never executed.
func Do(ctx context.Context, action func() error, options ...Option) error {
	var lastActionErr error
	for r := Start(ctx, options...); r.Next(); {
		lastActionErr = action()
		if lastActionErr == nil {
			return nil
		}
	}
	if lastActionErr == nil { // Context was done before action executed.
		return ctx.Err()
	}
	return lastActionErr
}

// Retrier allows controlling a retry loop.
//
// Note that an explict loop using a Retrier can be often replaced with simpler and less error-prone Do() function.
type Retrier interface {
	// Reset the retrier to its initial state, meaning that the next call to
	// Next will return immediately and subsequent calls will behave as if
	// they had followed the very first attempt.
	Reset()

	// Next returns whether the retry loop should continue, and blocks for the
	// appropriate length of time before yielding back to the caller.
	//
	// If a context is present, Next will eagerly return false if the context is done.
	Next() bool

	// CurrentAttempt returns current retry attempt.
	//
	// First attempt number is 0.
	//
	// Resetting retrier resets attempts counter.
	CurrentAttempt() int
}

// Option configures retry strategy such as backoff duration or maximal number of attempts.
type Option func(r *options)

// WithMaxAttempts sets upper limit on a number of attempts.
//
// Max attempts of 0 indicates no limit.
//
// If max attempts option is not used, then default value of 0 is used.
func WithMaxAttempts(maxAttempts int) Option {
	return func(o *options) {
		o.maxAttempts = maxAttempts
	}
}

// WithInitialBackoff sets initial backoff.
//
// If initial backoff option is not used, then default value of 50 milliseconds is used.
func WithInitialBackoff(initialBackoff time.Duration) Option {
	return func(o *options) {
		o.initialBackoff = initialBackoff
	}
}

// WithMaxBackoff sets upper limit on backoff duration.
//
// Max backoff of 0 indicates no limit.
//
// If max backoff option is not used, then default value of 2 seconds is used.
func WithMaxBackoff(maxBackoff time.Duration) Option {
	return func(o *options) {
		o.maxBackoff = maxBackoff
	}
}

// WithMultiplier sets backoff multiplier controlling how fast
// backoff duration grows with each retry attempt.
//
// If multiplier option is not used, then default value of 2 is used.
func WithMultiplier(multiplier float64) Option {
	return func(o *options) {
		o.multiplier = multiplier
	}
}

// WithRandomizationFactor sets randomization factor.
//
// If randomization factor option is not used, then default value of 0.15 is used.
func WithRandomizationFactor(randomizationFactor float64) Option {
	return func(o *options) {
		o.randomizationFactor = randomizationFactor
	}
}

// Start returns a new initialized retrier.
//
// If the provided context is canceled (see Context.Done), then Next() will eagerly return false and
// the retry loop will do no iterations.
func Start(ctx context.Context, opts ...Option) Retrier {
	r := &retrier{
		options: options{
			maxAttempts:         defaultMaxAttempts,
			initialBackoff:      defaultInitialBackoff,
			maxBackoff:          defaultMaxBackoff,
			multiplier:          defaultMultiplier,
			randomizationFactor: defaultRandomizationFactor,
		},
		ctxDoneChan:    ctx.Done(),
		currentAttempt: 0,
		isReset:        false,
	}
	for _, option := range opts {
		option(&r.options)
	}
	r.Reset()
	return r
}

const (
	defaultMaxAttempts         = 0 // Infinite retries.
	defaultInitialBackoff      = 50 * time.Millisecond
	defaultMaxBackoff          = 2 * time.Second
	defaultMultiplier          = 2.
	defaultRandomizationFactor = 0.15 // 15%
)

// retrier allows to control an exponential-backoff retry loop.
//
// Backoff after $attempt (first attempt is 0) is defined as:
//
//	backoff =
//	  min(initialBackoff * pow(multiplier, $attempt), maxBackoff == 0 ? +Inf : maxBackoff) *
//	    (1.0 - randomizationFactor + 2 * rand(0, randomizationFactor))
//
type retrier struct {
	options        options
	ctxDoneChan    <-chan struct{}
	currentAttempt int
	isReset        bool
}

type options struct {
	maxAttempts         int           // Maximum number of attempts (0 for infinite).
	initialBackoff      time.Duration // Default retry backoff interval.
	maxBackoff          time.Duration // Maximum retry backoff interval (0 for no max backoff).
	multiplier          float64       // Default backoff constant.
	randomizationFactor float64       // Randomize the backoff interval by constant.
}

func (r *retrier) Reset() {
	select {
	case <-r.ctxDoneChan:
		// When the context was canceled, you can't keep going.
		return
	default:
	}
	r.currentAttempt = 0
	r.isReset = true
}

func (r *retrier) Next() bool {
	if r.isReset {
		r.isReset = false
		return true
	}
	if r.options.maxAttempts > 0 && r.currentAttempt+1 >= r.options.maxAttempts {
		return false
	}
	// Wait before retry.
	select {
	case <-time.After(r.retryIn()):
		r.currentAttempt++
		return true
	case <-r.ctxDoneChan:
		return false
	}
}

func (r retrier) retryIn() time.Duration {
	backoff := float64(r.options.initialBackoff) * math.Pow(r.options.multiplier, float64(r.currentAttempt))
	if r.options.maxBackoff != 0 && backoff > float64(r.options.maxBackoff) {
		backoff = float64(r.options.maxBackoff)
	}

	var delta = r.options.randomizationFactor * backoff
	// Get a random value from the range [backoff - delta, backoff + delta].
	backoff = math.Trunc(backoff - delta + rand.Float64()*(2*delta) + 0.5)
	return time.Duration(backoff)
}

func (r retrier) CurrentAttempt() int {
	return r.currentAttempt
}
