package parallel

import (
	"container/list"
	"context"
	"fmt"
	"runtime"
	"sync"
)

/*
  This is a fork of sync.semaphore with modifications of high-priority resource grabber
*/

const OneCPU = 100.0

var AllCPUs = OneCPU * float64(runtime.NumCPU())

type waiter struct {
	n     int64
	ready chan<- struct{} // Closed when semaphore acquired.
}

func newCustomSem(n int64) *customSem {
	w := &customSem{size: n}
	return w
}

type customSem struct {
	size    int64
	cur     int64
	mu      sync.Mutex
	waiters list.List
}

// forceAcquire() guarantee acquires the quota instantly
func (s *customSem) forceAcquire(n int64) {
	s.mu.Lock()
	s.cur += n
	s.mu.Unlock()
}

// acquire() acquires the semaphore with a weight of n, blocking until resources
// are available or ctx is done. On success, returns nil. On failure, returns
// ctx.Err() and leaves the semaphore unchanged.
//
// If ctx is already done, Acquire may still succeed without blocking.
func (s *customSem) acquire(ctx context.Context, n int64) error {
	s.mu.Lock()
	if s.size-s.cur >= n && s.waiters.Len() == 0 {
		s.cur += n
		s.mu.Unlock()
		return nil
	}

	if n > s.size {
		// Don't make other Acquire calls block on one that's doomed to fail.
		s.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}

	ready := make(chan struct{})
	w := waiter{n: n, ready: ready}
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		s.mu.Lock()
		select {
		case <-ready:
			// Acquired the semaphore after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancelation.
			err = nil
		default:
			isFront := s.waiters.Front() == elem
			s.waiters.Remove(elem)
			// If we're at the front and there're extra tokens left, notify other waiters.
			if isFront && s.size > s.cur {
				s.notifyWaiters()
			}
		}
		s.mu.Unlock()
		return err

	case <-ready:
		return nil
	}
}

// release() releases the semaphore with a weight of n.
func (s *customSem) release(n int64) {
	s.mu.Lock()
	s.cur -= n
	if s.cur < 0 {
		s.mu.Unlock()
		panic("semaphore: released more than held")
	}
	s.notifyWaiters()
	s.mu.Unlock()
}

func (s *customSem) notifyWaiters() {
	for {
		next := s.waiters.Front()
		if next == nil {
			break // No more waiters blocked.
		}

		w := next.Value.(waiter)
		if s.size-s.cur < w.n {
			// Not enough tokens for the next waiter.  We could keep going (to try to
			// find a waiter with a smaller request), but under load that could cause
			// starvation for large requests; instead, we leave all remaining waiters
			// blocked.
			//
			// Consider a semaphore used as a read-write lock, with N tokens, N
			// readers, and one writer.  Each reader can Acquire(1) to obtain a read
			// lock.  The writer can Acquire(N) to obtain a write lock, excluding all
			// of the readers.  If we allow the readers to jump ahead in the queue,
			// the writer will starve — there is always one token available for every
			// reader.
			break
		}

		s.cur += w.n
		s.waiters.Remove(next)
		close(w.ready)
	}
}

var semMap = make(map[string]*customSem)

func Acquire(ctx context.Context, name string, units float64) {
	sem := semMap[name]
	if sem == nil {
		panic(fmt.Sprintf("missing call of InitQuota for [%s]", name))
	}
	_ = sem.acquire(ctx, int64(units))
}

func AcquireHighPriority(name string, units float64) {
	sem := semMap[name]
	if sem == nil {
		panic(fmt.Sprintf("missing call of InitQuota for [%s]", name))
	}
	sem.forceAcquire(int64(units))
}

func Release(name string, units float64) {
	sem := semMap[name]
	if sem == nil {
		panic(fmt.Sprintf("missing call of InitQuota for [%s]", name))
	}
	sem.release(int64(units))
}

// InitQuota is not thread-safe with all other functions in this module, suggest to be called at the beginning of the app
func InitQuota(name string, totalUnits float64) {
	semMap[name] = newCustomSem(int64(totalUnits))
}
