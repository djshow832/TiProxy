package event

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var MyEvent *Event
var once sync.Once

type Event struct {
	cond    sync.Cond
	waiting atomic.Int32
	total   atomic.Int32
}

func InitEvent(ctx context.Context) {
	once.Do(func() {
		MyEvent = &Event{
			cond: sync.Cond{L: &sync.Mutex{}},
		}
		go MyEvent.Start(ctx)
	})
}

func (e *Event) Increase() {
	e.total.Add(1)
}

func (e *Event) Decrease() {
	e.total.Add(-1)
}

func (e *Event) WaitForEvent() {
	e.cond.L.Lock()
	e.waiting.Add(1)
	e.cond.Wait()
	e.waiting.Add(-1)
	e.cond.L.Unlock()
}

// If possible, we can set the priority of this goroutine to lowest so that we don't need total and waiting.
// Before Broadcast, epoll has more possibility to run because few goroutines are runnable.
func (e *Event) Start(ctx context.Context) {
	procs := int32(runtime.GOMAXPROCS(0))
	ticker := time.NewTicker(100 * time.Microsecond)
	for ctx.Err() == nil {
		total := e.total.Load()
		waiting := e.waiting.Load()
		if waiting > 0 && total-waiting <= procs {
			e.cond.Broadcast()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
	ticker.Stop()
}
