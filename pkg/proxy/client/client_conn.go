// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/cloudwego/netpoll"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/proxy/poll"
	"go.uber.org/zap"
)

type ClientConnection struct {
	connID            uint64
	logger            *zap.Logger
	frontendTLSConfig *tls.Config    // the TLS config to connect to clients.
	backendTLSConfig  *tls.Config    // the TLS config to connect to TiDB server.
	pkt               *pnet.PacketIO // a helper to read and write data in packet format.
	connMgr           *backend.BackendConnManager
}

func NewClientConnection(logger *zap.Logger, conn net.Conn, frontendTLSConfig *tls.Config, backendTLSConfig *tls.Config,
	hsHandler backend.HandshakeHandler, connID uint64, addr string, bcConfig *backend.BCConfig) *ClientConnection {
	bemgr := backend.NewBackendConnManager(logger.Named("be"), hsHandler, connID, bcConfig)
	bemgr.SetValue(backend.ConnContextKeyConnAddr, addr)
	opts := make([]pnet.PacketIOption, 0, 2)
	opts = append(opts, pnet.WithWrapError(backend.ErrClientConn))
	if bcConfig.ProxyProtocol {
		opts = append(opts, pnet.WithProxy)
	}
	pkt := pnet.NewPacketIO(conn, logger, bcConfig.ConnBufferSize, opts...)
	return &ClientConnection{
		connID:            connID,
		logger:            logger,
		frontendTLSConfig: frontendTLSConfig,
		backendTLSConfig:  backendTLSConfig,
		pkt:               pkt,
		connMgr:           bemgr,
	}
}

func (cc *ClientConnection) ConnID() uint64 {
	return cc.connID
}

func (cc *ClientConnection) Connect(ctx context.Context, conn netpoll.Connection) {
	err := cc.connMgr.Connect(ctx, cc.pkt, cc.frontendTLSConfig, cc.backendTLSConfig)
	if err != nil {
		cc.clean("new connection failed", err, conn)
		return
	}
	cc.logger.Debug("connected to backend", cc.connMgr.ConnInfo()...)
}

func (cc *ClientConnection) processMsg(ctx context.Context) error {
	cc.pkt.ResetSequence()
	clientPkt, err := cc.pkt.ReadPacket()
	if err != nil {
		return err
	}
	err = cc.connMgr.ExecuteCmd(ctx, clientPkt)
	if err != nil {
		return err
	}
	if pnet.Command(clientPkt[0]) == pnet.ComQuit {
		return nil
	}
	return nil
}

func (cc *ClientConnection) clean(msg string, err error, conn netpoll.Connection) {
	src := cc.connMgr.QuitSource()
	if !src.Normal() {
		fields := cc.connMgr.ConnInfo()
		fields = append(fields, zap.Stringer("quit_source", src), zap.Error(err))
		cc.logger.Warn(msg, fields...)
	}
	metrics.DisConnCounter.WithLabelValues(src.String()).Inc()
	_ = conn.Close()
}

func (cc *ClientConnection) GracefulClose() {
	cc.connMgr.GracefulClose()
}

func (cc *ClientConnection) Close() error {
	return errors.Collect(ErrCloseConn, cc.pkt.Close(), cc.connMgr.Close())
}

func OnRequest(ctx context.Context, conn netpoll.Connection) error {
	cc := ctx.Value(poll.CtxKeyClientConn).(*ClientConnection)
	err := cc.processMsg(ctx)
	if err != nil {
		cc.clean("fails to relay the connection", err, conn)
	}
	return err
}
