// Package `batch` provides a way to run a number of goroutines concurrently.
// Goroutines are executed in batches allowing for result processing after each
// batch.
// It has a similar API to https://pkg.go.dev/golang.org/x/sync/errgroup.
package batch

import (
	"sync"
)

// A Group is a collection of goroutines working the same overall task batched together.
type Group[T any] struct {
	wg      sync.WaitGroup
	cb      func([]T) error
	errOnce sync.Once
	err     error
	res     chan T
	calls   uint64
}

// New creates a new batch group with limit >= 1 number of concurrent goroutines.
// The callback function is called when the number of completed goroutines reaches limit with
// the returned values of all goroutines if no error occured.
// v will always be 0 < len(v) <= limit.
// The callback function can return an error that will turn all subsequent calls to Go into
// a no-op and be returned by Wait.
func New[T any](limit int, cb func(v []T) error) Group[T] {
	if limit <= 0 {
		limit = 1
	}
	return Group[T]{
		cb:  cb,
		res: make(chan T, limit),
	}
}

// Go calls the given function in a new goroutine.
// If the number of goroutines reached the limit, it first waits for all goroutines to finish
// and calls the callback function if no errors occured.
// Go becomes a no-op if an error occured.
func (g *Group[T]) Go(f func() (T, error)) {
	if g.err != nil {
		// An error occured in the previous batch.
		// Turn Go into a no-op and let the caller call Wait.
		return
	}
	if g.calls > 0 && g.calls%uint64(cap(g.res)) == 0 {
		g.wg.Wait()
		res := []T{}
		for len(g.res) > 0 {
			res = append(res, <-g.res) // Drain the channel.
		}
		if g.err != nil {
			// An error occured in the current batch.
			// Turn Go into a no-op and let the caller call Wait.
			return
		}
		if g.err = g.cb(res); g.err != nil {
			// An error occured in the callback.
			// Turn Go into a no-op and let the caller call Wait.
			return
		}
	}
	g.calls++
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		v, err := f()
		if err != nil {
			g.errOnce.Do(func() {
				g.err = err
			})
		}
		g.res <- v
	}()
}

// Wait waits for all leftover goroutines to finish and calls the callback function if
// necessary.
// Wait MUST be called after all Go calls to ensure there's no leftover goroutines.
// The error returned will be the first error that occured.
func (g *Group[T]) Wait() error {
	g.wg.Wait()
	res := []T{}
	for len(g.res) > 0 {
		res = append(res, <-g.res)
	}
	if g.err != nil {
		// return the first error if any.
		return g.err
	}
	if len(res) > 0 {
		return g.cb(res)
	}
	return nil
}
