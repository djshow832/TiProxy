// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package poll

import (
	"context"
	"net"
	"time"

	"github.com/cloudwego/netpoll"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
)

const (
	CtxKeyClientConn = "client_conn"

	goPoolSize = 100
	goMaxIdle  = time.Minute
)

var wgp = waitgroup.NewWaitGroupPool(goPoolSize, goMaxIdle)

func CreateListener(network, address string) (netpoll.Listener, error) {
	return netpoll.CreateListener(network, address)
}

func Run(listener net.Listener, onConnect netpoll.OnConnect, onRequest netpoll.OnRequest, onDisconnect netpoll.OnDisconnect) error {
	netpoll.SetRunner(func(ctx context.Context, f func()) {
		wgp.RunWithRecover(f, nil, nil)
	})
	eventLoop, err := netpoll.NewEventLoop(
		onRequest,
		netpoll.WithOnConnect(onConnect),
		netpoll.WithOnDisconnect(onDisconnect),
	)
	if err != nil {
		return err
	}
	eventLoop.Serve(listener)
	return nil
}

func DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	return netpoll.DialConnection(network, address, timeout)
}
