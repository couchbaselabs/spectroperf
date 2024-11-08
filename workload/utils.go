package workload

import (
	"go.uber.org/zap"
	"math/rand"
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
