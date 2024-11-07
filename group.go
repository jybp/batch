package batch

import (
	"sync"
	"sync/atomic"
)

// A Group is a collection of goroutines working the same overall task batched together.
type Group[T any] struct {
	cb       func([]T, error) error
	wg       sync.WaitGroup
	errOnce  sync.Once
	err      error
	hasCbErr atomic.Bool
	res      chan T
	calls    uint64
}

// New creates a new batch group with limit >= 1 number of concurrent goroutines.
// The callback function is called when the number of completed goroutines reaches limit with
// the returned values of all goroutines and any first error that occured.
// len(v) will always be > 0.
// The callback function can return an error that will turn all subsequent calls to Go into
// a no-op and be returned by Wait.
func New[T any](limit int, cb func(v []T, err error) error) Group[T] {
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
// and calls the callback function.
// Go is a no-op if an error occured in the previous callback.
func (g *Group[T]) Go(f func() (T, error)) {
	if g.hasCbErr.Load() {
		// An error occured in the previous batch.
		// Do not invoke the callback function, let the caller call Wait and turn Go into
		// a no-op.
		return
	}
	if c := atomic.LoadUint64(&g.calls); c > 0 && c%uint64(cap(g.res)) == 0 {
		g.wg.Wait()
		res := []T{}
		for len(g.res) > 0 {
			res = append(res, <-g.res)
		}
		if g.err = g.cb(res, g.err); g.err != nil {
			g.hasCbErr.Store(true)
			return
		}
	}
	atomic.AddUint64(&g.calls, 1)
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

// Wait waits for all leftover goroutines to finish and calls the callback function.
// Wait *must* be called after all Go calls to ensure there's no leftover goroutines.
// The error returned will be the first callback error.
func (g *Group[T]) Wait() error {
	g.wg.Wait()
	res := []T{}
	for len(g.res) > 0 {
		res = append(res, <-g.res)
	}
	if len(res) > 0 {
		if err := g.cb(res, g.err); err != nil {
			return err
		}
	}
	if g.hasCbErr.Load() {
		return g.err
	}
	return nil
}
