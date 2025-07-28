package workloads

import (
	"context"
	"fmt"

	"github.com/brianvoe/gofakeit"
	gotel "github.com/couchbase/gocb-opentelemetry"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/workload"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type basic struct {
	logger     *zap.Logger
	numItems   int
	bucket     string
	scope      *gocb.Scope
	collection *gocb.Collection
	cluster    *gocb.Cluster
}

func NewBasic(
	logger *zap.Logger,
	bucket string,
	numItems int,
	scope *gocb.Scope,
	collection *gocb.Collection,
	cluster *gocb.Cluster) basic {

	return basic{
		logger:     logger,
		numItems:   numItems,
		bucket:     bucket,
		scope:      scope,
		collection: collection,
		cluster:    cluster,
	}
}

func (w basic) Operations() []string {
	return []string{"get", "set", "query", "fullTextSearch"}
}

func (w basic) Probabilities() [][]float64 {
	return [][]float64{
		{0.25, 0.25, 0.25, 0.25},
		{0.25, 0.25, 0.25, 0.25},
		{0.25, 0.25, 0.25, 0.25},
		{0.25, 0.25, 0.25, 0.25},
	}
}

func (w basic) Functions() map[string]func(ctx context.Context, rctx workload.Runctx) error {
	return map[string]func(ctx context.Context, rctx workload.Runctx) error{
		"get":            w.get,
		"set":            w.set,
		"query":          w.query,
		"fullTextSearch": w.fullTextSearch,
	}
}

// Create a simple random document of the form:
//
//	{
//		"id": 123,
//		"randomString": "aaiehgmosje",
//	}
func (w basic) GenerateDocument(id int) workload.DocType {
	firstLetter := string(rune((id % 25) + 97))
	randString := gofakeit.Lexify(firstLetter + "??????????")

	doc := Doc{
		Id:         id,
		RandString: randString,
	}

	return workload.DocType{
		Name: fmt.Sprintf("%d", id),
		Data: doc,
	}
}

func (w basic) Setup() error {
	gofakeit.Seed(int64(workload.RandSeed))

	err := CreateBasicQueryIndex(w.logger, w.collection)
	if err != nil {
		return err
	}

	// err = EnsureBasicFtsIndex(w.logger, w.cluster)
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (w basic) get(ctx context.Context, rctx workload.Runctx) error {
	id := rctx.Rand().Int31n(int32(w.numItems))

	span := trace.SpanFromContext(ctx)
	_, err := w.collection.Get(fmt.Sprintf("%d", id), &gocb.GetOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
	if err != nil {
		return fmt.Errorf("get failed: %s", err.Error())
	}
	return nil
}

func (w basic) set(ctx context.Context, rctx workload.Runctx) error {
	id := rctx.Rand().Int31n(int32(w.numItems))
	span := trace.SpanFromContext(ctx)
	newDoc := w.GenerateDocument(int(id))

	_, uerr := w.collection.Upsert(fmt.Sprintf("%d", id), newDoc, &gocb.UpsertOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
	if uerr != nil {
		return fmt.Errorf("data load upsert failed: %s", uerr.Error())
	}

	return nil
}

func (w basic) query(ctx context.Context, rctx workload.Runctx) error {
	return nil
}

func (w basic) fullTextSearch(ctx context.Context, rctx workload.Runctx) error {
	return nil
}
