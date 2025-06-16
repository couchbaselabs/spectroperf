package workload

import (
	"math/rand"

	"github.com/couchbase/gocb/v2"
	"go.uber.org/zap"
)

var RandSeed = 11211

type DocType struct {
	Name string
	Data interface{}
}

type Runctx struct {
	r rand.Rand
	l zap.Logger
}

func (r Runctx) Rand() *rand.Rand {
	return &r.r
}

func (r Runctx) Logger() *zap.Logger {
	return &r.l
}

type NoRetyStrategy struct {
}

func (rs *NoRetyStrategy) RetryAfter(req gocb.RetryRequest, reason gocb.RetryReason) gocb.RetryAction {
	return &gocb.NoRetryRetryAction{}
}
