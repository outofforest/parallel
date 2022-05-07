package parallel

import (
	"context"
	"fmt"
	"runtime/debug"
)

// PanicError is the error type that occurs when a subtask panics
type PanicError struct {
	Value interface{}
	Stack []byte
}

func (err PanicError) Error() string {
	return fmt.Sprintf("panic: %s", err.Value)
}

// Unwrap returns the error passed to panic, or nil if panic was called with
// something other than an error
func (err PanicError) Unwrap() error {
	if e, ok := err.Value.(error); ok {
		return e
	}
	return nil
}

// runTask executes the task in the current goroutine, recovering from panics.
// A panic is returned as PanicError.
func runTask(ctx context.Context, task Task) (err error) {
	defer func() {
		if p := recover(); p != nil {
			panicErr := PanicError{Value: p, Stack: debug.Stack()}
			err = panicErr
		}
	}()
	return task(ctx)
}
