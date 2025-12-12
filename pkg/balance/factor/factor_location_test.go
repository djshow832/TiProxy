// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFactorLocationScore(t *testing.T) {
	tests := []struct {
		local         bool
		expectedScore uint64
	}{
		{
			local:         false,
			expectedScore: 1,
		},
		{
			local:         true,
			expectedScore: 0,
		},
	}

	factor := NewFactorLocation()
	backends := make([]scoredBackend, 0, len(tests))
	for _, test := range tests {
		backends = append(backends, scoredBackend{
			BackendCtx: &mockBackend{
				local: test.local,
			},
		})
	}
	factor.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.expectedScore, backends[i].score(), "test idx: %d", i)
	}
}

func TestFactorLocationConfig(t *testing.T) {
	tests := []struct {
		local []bool
		speed float64
	}{
		{
			local: []bool{true, true},
			speed: 0,
		},
		{
			local: []bool{true, false},
			speed: 10,
		},
	}

	factor := NewFactorLocation()
	factor.SetConfig(&config.Config{Balance: config.Balance{Location: config.Factor{MigrationsPerSecond: 10}}})
	require.EqualValues(t, 10, factor.migrationsPerSecond)
	backend1 := newMockBackend(true, 0)
	backend2 := newMockBackend(true, 0)
	scoredBackend1 := newScoredBackend(backend1, zap.NewNop())
	scoredBackend2 := newScoredBackend(backend2, zap.NewNop())
	for i, test := range tests {
		backend1.connScore = test.score1
		backend2.connScore = test.score2
		_, balanceCount, _ := factor.BalanceCount(scoredBackend1, scoredBackend2)
		require.EqualValues(t, balanceCount, test.speed, "case id: %d", i)
	}
}
