package workloads

import (
	"context"
	"fmt"

	"github.com/brianvoe/gofakeit"
	gotel "github.com/couchbase/gocb-opentelemetry"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/search"
	"github.com/couchbaselabs/spectroperf/configuration"
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

func NewBasic(logger *zap.Logger, config *configuration.Config, cluster *gocb.Cluster) basic {
	scope := cluster.Bucket(config.Bucket).Scope(config.Scope)

	return basic{
		logger:     logger,
		numItems:   config.NumItems,
		bucket:     config.Bucket,
		scope:      scope,
		collection: scope.Collection(config.Collection),
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

	err = EnsureBasicFtsIndex(w.logger, w.cluster)
	if err != nil {
		return err
	}

	return nil
}

func (w basic) get(ctx context.Context, rctx workload.Runctx) error {
	id := fmt.Sprintf("%d", rctx.Rand().Int31n(int32(w.numItems)))
	span := trace.SpanFromContext(ctx)
	_, err := w.collection.Get(id, &gocb.GetOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
	if err != nil {
		return fmt.Errorf("get operation failed: %s", err.Error())
	}
	return nil
}

func (w basic) set(ctx context.Context, rctx workload.Runctx) error {
	id := rctx.Rand().Int31n(int32(w.numItems))
	span := trace.SpanFromContext(ctx)
	newDoc := w.GenerateDocument(int(id))

	_, err := w.collection.Upsert(fmt.Sprintf("%d", id), newDoc.Data, &gocb.UpsertOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
	if err != nil {
		return fmt.Errorf("set operation failed: %s", err.Error())
	}
	return nil
}

func (w basic) query(ctx context.Context, rctx workload.Runctx) error {
	span := trace.SpanFromContext(ctx)
	toFind := fmt.Sprintf("%s%%", gofakeit.Letter())
	query := fmt.Sprintf("SELECT * FROM %s WHERE RandString LIKE '%s' LIMIT 1", w.collection.Name(), toFind)

	rows, err := w.scope.Query(query, &gocb.QueryOptions{
		Adhoc:         true,
		ParentSpan:    gotel.NewOpenTelemetryRequestSpan(ctx, span),
		RetryStrategy: &workload.NoRetyStrategy{},
	})
	if err != nil {
		return fmt.Errorf("query operation failed: %s", err.Error())
	}

	for rows.Next() {
		var resp UserQueryResponse
		err := rows.Row(&resp)
		if err != nil {
			return fmt.Errorf("reading next row: %s", err.Error())
		}
	}

	err = rows.Err()
	if err != nil {
		return fmt.Errorf("iterating rows: %s", err.Error())
	}
	return nil
}

func (w basic) fullTextSearch(ctx context.Context, rctx workload.Runctx) error {
	span := trace.SpanFromContext(ctx)
	toFind := fmt.Sprintf("%s*", gofakeit.Letter())

	matchResult, err := w.cluster.SearchQuery(
		"rand-string-index",
		search.NewMatchQuery(toFind),
		&gocb.SearchOptions{
			ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span),
		},
	)

	if err != nil {
		return fmt.Errorf("fts query failed: %s", err.Error())
	}

	var matchingDocs []string
	for matchResult.Next() {
		row := matchResult.Row()
		matchingDocs = append(matchingDocs, row.ID)
	}

	err = matchResult.Err()
	if err != nil {
		return fmt.Errorf("iterating rows: %s", err.Error())
	}

	return nil
}
