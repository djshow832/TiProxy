package event

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiproxy/pkg/util/monotime"
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

// If all goroutines are waiting and procs goroutines are waiting for epoll, epoll will run.
func (e *Event) Start(ctx context.Context) {
	listener, _ := net.Listen("tcp", "localhost:0")
	defer listener.Close()
	ch := make(chan []byte, 1)
	go func() {
		conn, _ := listener.Accept()
		defer conn.Close()
		for ctx.Err() == nil {
			data := <-ch
			conn.Write(data)
		}
	}()
	conn, _ := net.Dial("tcp", listener.Addr().String())
	defer conn.Close()
	ticker := time.NewTicker(40 * time.Microsecond)
	defer ticker.Stop()
	data := make([]byte, 1)
	lastTime := monotime.Time(0)
	for ctx.Err() == nil {
		total := e.total.Load()
		waiting := e.waiting.Load()
		now := monotime.Now()
		if waiting > total/2 || time.Duration(now-lastTime) > 400*time.Microsecond {
			ch <- []byte{1}
			conn.Read(data)
			e.cond.Broadcast()
			lastTime = now
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *Event) waitWithRead(ctx context.Context) {
	listener, _ := net.Listen("tcp", "localhost:0")
	defer listener.Close()
	ch := make(chan []byte, 1)
	go func() {
		conn, _ := listener.Accept()
		defer conn.Close()
		for ctx.Err() == nil {
			data := <-ch
			conn.Write(data)
		}
	}()
	conn, _ := net.Dial("tcp", listener.Addr().String())
	defer conn.Close()
	ticker := time.NewTicker(20 * time.Microsecond)
	defer ticker.Stop()
	data := make([]byte, 1)
	for ctx.Err() == nil {
		waiting := e.waiting.Load()
		if waiting > 0 {
			ch <- []byte{1}
			conn.Read(data)
			e.cond.Broadcast()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
