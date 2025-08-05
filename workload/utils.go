package workload

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/pkg/errors"
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

func WaitForIndexToBuild(mgr *gocb.CollectionQueryIndexManager, logger *zap.Logger, indexName string) error {
	logger.Info("waiting for index to build", zap.String("name", indexName))
	for {
		indexes, err := mgr.GetAllIndexes(nil)
		if err != nil {
			return errors.Wrap(err, "failed to get all indexes")
		}

		for _, index := range indexes {
			if index.Name == indexName {
				switch index.State {
				case "online":
					logger.Info("index is online", zap.String("name", indexName))
					return nil
				case "building":
					logger.Info("index still building", zap.String("name", indexName))
					time.Sleep(time.Second * 30)
				default:
					return fmt.Errorf("unexpected index state: %s", index.State)
				}
			}
		}
	}
}
