package workloads

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/configuration"
	"github.com/couchbaselabs/spectroperf/workload"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

type basicDapi struct {
	connstr    string
	logger     *zap.Logger
	username   string
	password   string
	client     *http.Client
	numItems   int
	bucket     string
	scope      string
	collection *gocb.Collection
	cluster    *gocb.Cluster
}

type Doc struct {
	Id         int
	RandString string
}

func NewBasicDapi(logger *zap.Logger, config *configuration.Config, cluster *gocb.Cluster) basicDapi {
	tr := otelhttp.NewTransport(
		&http.Transport{
			MaxConnsPerHost:     500,
			MaxIdleConnsPerHost: 100,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: config.TlsSkipVerify},
		},
		// By setting the otelhttptrace client in this transport, it can be
		// injected into the context after the span is started, which makes the
		// httptrace spans children of the transport one.
		otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace {
			return otelhttptrace.NewClientTrace(ctx)
		}),
	)

	scope := cluster.Bucket(config.Bucket).Scope(config.Scope)

	return basicDapi{
		connstr:    config.DapiConnstr,
		logger:     logger,
		username:   config.Username,
		password:   config.Password,
		client:     &http.Client{Transport: tr},
		numItems:   config.NumItems,
		bucket:     config.Bucket,
		scope:      config.Scope,
		collection: scope.Collection(config.Collection),
		cluster:    cluster,
	}
}

func (w basicDapi) Operations() []string {
	return []string{"get", "set", "query", "fullTextSearch"}
}

func (w basicDapi) Probabilities() [][]float64 {
	return [][]float64{
		{0.25, 0.25, 0.25, 0.25},
		{0.25, 0.25, 0.25, 0.25},
		{0.25, 0.25, 0.25, 0.25},
		{0.25, 0.25, 0.25, 0.25},
	}
}

func (w basicDapi) Functions() map[string]func(ctx context.Context, rctx workload.Runctx) error {
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
func (w basicDapi) GenerateDocument(id int) workload.DocType {
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

func (w basicDapi) Setup() error {
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

func (w basicDapi) executeRequest(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(w.username, w.password)
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute get request: %s", err.Error())
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("profile fetch returned unexpected status code %d", resp.StatusCode)
	}
	return resp, nil
}

func (w basicDapi) get(ctx context.Context, rctx workload.Runctx) error {
	id := rctx.Rand().Int31n(int32(w.numItems))
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%d", w.connstr, w.bucket, w.scope, w.collection.Name(), id)
	req, err := http.NewRequestWithContext(ctx, "GET", requestURL, nil)
	if err != nil {
		panic(fmt.Errorf("failed to build fetch request: %s", err.Error()))
	}

	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not fetch doc: %s", err.Error())
	}

	bodyText, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("could not close response body: %s", err.Error())
	}

	var doc Doc
	err = json.Unmarshal(bodyText, &doc)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyText), err.Error())
	}
	return nil
}

func (w basicDapi) set(ctx context.Context, rctx workload.Runctx) error {
	id := rctx.Rand().Int31n(int32(w.numItems))
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%d", w.connstr, w.bucket, w.scope, w.collection.Name(), id)

	newDoc := w.GenerateDocument(int(id))
	jsonBytes, err := json.Marshal(newDoc.Data)
	if err != nil {
		return fmt.Errorf("could not marshal newDoc to json: &s", err.Error())
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", requestURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		panic(fmt.Errorf("building set request: %s", err.Error()))
	}

	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("executing set request: %s", err.Error())
	}

	// Read response body so that http request span is correctly ended
	_, _ = io.ReadAll(resp.Body)
	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("closing response body: %s", err.Error())
	}

	return nil
}

type BasicDapiQueryResponse struct {
	Results []BasicQueryResponse `json:"results"`
}

type BasicQueryResponse struct {
	Profiles Doc
}

func (w basicDapi) query(ctx context.Context, rctx workload.Runctx) error {
	toFind := fmt.Sprintf("%s%%", gofakeit.Letter())
	query := fmt.Sprintf("SELECT * FROM %s.%s.%s WHERE RandString LIKE '%s' LIMIT 1", w.bucket, w.scope, w.collection.Name(), toFind)
	payload := DapiQueryPayload{
		Statement: query,
	}
	requestURL := fmt.Sprintf("%s/_p/query/query/service", w.connstr)
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", requestURL, bytes.NewBuffer(body))
	if err != nil {
		panic(fmt.Errorf("building query request: %s", err.Error()))
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("executing query request: %s", err.Error())
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("closing response body: %s", err.Error())
	}

	var results BasicDapiQueryResponse
	err = json.Unmarshal(bodyBytes, &results)
	if err != nil {
		return fmt.Errorf("unmarshalling response body - %s : %s", string(bodyBytes), err.Error())
	}
	return nil
}

func (w basicDapi) fullTextSearch(ctx context.Context, rctx workload.Runctx) error {
	toFind := fmt.Sprintf("%s*", gofakeit.Letter())

	rctx.Logger().Sugar().Debugf("Finding docs where rand string contains %s", toFind)

	requestURL := fmt.Sprintf("%s/_p/fts/api/index/rand-string-index/query", w.connstr)
	payload := DapiSearchQueryPayload{
		Query: SearchQuery{
			Query: toFind,
		},
	}

	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", requestURL, bytes.NewBuffer(body))
	if err != nil {
		panic(fmt.Errorf("building fts request: %s", err.Error()))
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("executing fts request: %s", err.Error())
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body response body: %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("closing response body: %s", err.Error())
	}

	var results DapiUserSearchQueryResponse
	err = json.Unmarshal(bodyBytes, &results)
	if err != nil {
		return fmt.Errorf("unmarshalling response body - %s : %s", string(bodyBytes), err.Error())
	}

	var matchingDocs []string
	for _, result := range results.Results {
		matchingDocs = append(matchingDocs, result.Id)
	}

	rctx.Logger().Sugar().Debugf("Found %d docs matching %s\n", len(matchingDocs), toFind)

	return nil
}

func CreateBasicQueryIndex(logger *zap.Logger, collection *gocb.Collection) error {
	indexName := "basicIndex"

	logger.Info("creating query index", zap.String("name", indexName))

	mgr := collection.QueryIndexes()
	err := mgr.CreateIndex(indexName, []string{"id"}, &gocb.CreateQueryIndexOptions{
		IgnoreIfExists: true,
	})

	switch {
	case err == nil:
	case errors.Is(err, gocb.ErrAmbiguousTimeout):
		return workload.WaitForIndexToBuild(mgr, logger, indexName)
	case errors.Is(err, gocb.ErrServiceNotAvailable):
		logger.Warn("query service not available on cluster, any query operations will fail")
	default:
		return errors.Wrap(err, fmt.Sprintf("failed to create %s", indexName))
	}

	return nil
}

func EnsureBasicFtsIndex(logger *zap.Logger, cluster *gocb.Cluster) error {
	indexName := "rand-string-index"
	mgr := cluster.SearchIndexes()

	_, err := mgr.GetIndex(indexName, nil)
	switch {
	case err == nil:
		logger.Info("skipping fts index creation as already present", zap.String("name", indexName))
		return nil
	case errors.Is(err, gocb.ErrIndexNotFound):
	case errors.Is(err, gocb.ErrServiceNotAvailable):
		logger.Warn("search service not available on cluster, any fts operations will fail")
		return nil
	default:
		return err
	}

	logger.Info("creating fts index", zap.String("name", indexName))

	params := map[string]interface{}{
		"doc_config": map[string]interface{}{
			"mode":       "scope.collection.type_field",
			"type_field": "RandString",
		},
		"mapping": map[string]interface{}{
			"default_mapping": map[string]interface{}{
				"dynamic": true,
				"enabled": false,
			},
			"types": map[string]interface{}{
				"identity.profiles": map[string]interface{}{
					"dynamic": false,
					"enabled": true,
					"properties": map[string]interface{}{
						"RandString": map[string]interface{}{
							"dynamic": false,
							"enabled": true,
							"fields": []any{
								map[string]any{
									"name":                 "RandString",
									"type":                 "text",
									"store":                true,
									"index":                true,
									"include_term_vectors": true,
									"include_in_all":       true,
								},
							},
						},
					},
				},
			},
		},
	}

	err = mgr.UpsertIndex(gocb.SearchIndex{
		UUID:         "",
		Name:         indexName,
		SourceName:   "data",
		Type:         "fulltext-index",
		Params:       params,
		SourceUUID:   "",
		SourceParams: nil,
		SourceType:   "gocbcore",
		PlanParams:   nil,
	}, nil)
	if err != nil {
		return err
	}

	logger.Info("checking fts index is ready to use", zap.String("name", indexName))

	var finalError error
	end := time.Now().Add(time.Minute)
	for time.Now().Before(end) {
		count, err := mgr.GetIndexedDocumentsCount(indexName, nil)
		if count > 0 {
			return nil
		}

		finalError = err
		logger.Info("waiting for documents to be indexed ", zap.String("name", indexName))
		time.Sleep(time.Second * 10)
	}

	return fmt.Errorf("timed out waiting for fts index to be ready: %w", finalError)
}
