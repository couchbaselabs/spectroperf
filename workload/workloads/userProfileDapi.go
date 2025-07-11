package workloads

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/configuration"
	"github.com/couchbaselabs/spectroperf/workload"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

type userProfileDapi struct {
	logger     *zap.Logger
	connstr    string
	username   string
	password   string
	client     *http.Client
	numItems   int
	bucket     string
	scope      string
	collection *gocb.Collection
	cluster    *gocb.Cluster
}

func NewUserProfileDapi(
	logger *zap.Logger,
	config *configuration.Config,
	collection *gocb.Collection,
	cluster *gocb.Cluster) userProfileDapi {
	tr := otelhttp.NewTransport(
		&http.Transport{
			MaxConnsPerHost:     500,
			MaxIdleConnsPerHost: 100,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.TlsSkipVerify,
			},
		},
		// By setting the otelhttptrace client in this transport, it can be
		// injected into the context after the span is started, which makes the
		// httptrace spans children of the transport one.
		otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace {
			return otelhttptrace.NewClientTrace(ctx)
		}),
	)

	return userProfileDapi{
		logger:     logger,
		connstr:    config.DapiConnstr,
		username:   config.Username,
		password:   config.Password,
		client:     &http.Client{Transport: tr},
		numItems:   config.NumItems,
		bucket:     config.Bucket,
		scope:      config.Scope,
		collection: collection,
		cluster:    cluster,
	}
}

// Create a random document with a realistic size from name, email, status text and whether
// or not the account is enabled.
func (w userProfileDapi) GenerateDocument(id int) workload.DocType {
	rng := rand.NewSource(int64(workload.RandSeed))
	r := rand.New(rng)

	var interests string
	numberOfInterests := rand.Intn(10)
	for i := 0; i < numberOfInterests; i++ {
		interest := Interests[rand.Intn(len(Interests))]

		if i == 0 {
			interests = interest
		} else {
			if !strings.Contains(interests, interest) {
				interests = interests + ", " + interest
			}
		}
	}

	iu := User{
		Name:      gofakeit.Name(),
		Email:     gofakeit.Email(), // TODO: make the email actually based on the name (pedantic)
		Created:   gofakeit.DateRange(time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)),
		Status:    gofakeit.Paragraph(1, r.Intn(8)+1, r.Intn(12)+1, "\n"),
		Interests: interests,
		Enabled:   true,
	}

	return workload.DocType{
		Name: fmt.Sprintf("u%d", id),
		Data: iu,
	}
}

func (w userProfileDapi) Operations() []string {
	return []string{"fetchProfile", "updateProfile", "lockProfile", "findProfile", "findRelatedProfiles"}
}

func (w userProfileDapi) Probabilities() [][]float64 {
	return [][]float64{
		{0, 0.7, 0.1, 0.15, 0.05},
		{0.8, 0, 0.1, 0.05, 0.05},
		{0.7, 0.2, 0, 0.05, 0.05},
		{0.6, 0.2, 0.15, 0, 0.05},
		{0.6, 0.2, 0.15, 0.05, 0},
	}
}

func (w userProfileDapi) Setup() error {
	gofakeit.Seed(int64(workload.RandSeed))

	err := CreateQueryIndex(w.logger, w.collection)
	if err != nil {
		return err
	}

	err = EnsureFtsIndex(w.logger, w.cluster, w.bucket, w.scope, w.collection.Name())
	if err != nil {
		return err
	}

	return nil
}

func (w userProfileDapi) Functions() map[string]func(ctx context.Context, rctx workload.Runctx) error {
	return map[string]func(ctx context.Context, rctx workload.Runctx) error{
		"fetchProfile":        w.fetchProfile,        // similar to login or looking at someone
		"updateProfile":       w.updateProfile,       // updating a status on the profile
		"lockProfile":         w.lockProfile,         // disable or enable a random profile (account lockout)
		"findProfile":         w.findProfile,         // find a profile by a secondary index (email address)
		"findRelatedProfiles": w.findRelatedProfiles, // look for people with similar interests
	}
}

func (w userProfileDapi) executeRequest(req *http.Request) (*http.Response, error) {
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

// Fetch a random profile in the range of profiles
func (w userProfileDapi) fetchProfile(ctx context.Context, rctx workload.Runctx) error {
	id := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems)))
	w.logger.Debug("fetching profile", zap.String("id", id))

	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", w.connstr, w.bucket, w.scope, w.collection.Name(), id)
	req, err := http.NewRequestWithContext(ctx, "GET", requestURL, nil)
	if err != nil {
		panic(fmt.Errorf("failed to build profile fetch request: %s", err.Error()))
	}

	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not fetch profile to update: %s", err.Error())
	}

	bodyText, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("could not close response body: %s", err.Error())
	}

	var toUd User
	err = json.Unmarshal(bodyText, &toUd)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyText), err.Error())
	}
	return nil
}

// Update the status of a random profile
func (w userProfileDapi) updateProfile(ctx context.Context, rctx workload.Runctx) error {
	id := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems)))
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", w.connstr, w.bucket, w.scope, w.collection.Name(), id)
	req, err := http.NewRequestWithContext(ctx, "GET", requestURL, nil)
	if err != nil {
		panic(fmt.Errorf("failed to build profile fetch request: %s", err.Error()))
	}

	w.logger.Debug("getting profile to update", zap.String("id", id))
	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not fetch profile to update: %s", err.Error())
	}

	bodyText, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("could not close response body: %s", err.Error())
	}

	var toUd User
	err = json.Unmarshal(bodyText, &toUd)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyText), err.Error())
	}

	toUd.Status = gofakeit.Paragraph(1, rctx.Rand().Intn(8)+1, rctx.Rand().Intn(12)+1, "\n")

	jsonBytes, err := json.Marshal(toUd)
	if err != nil {
		return fmt.Errorf("could not marshal User to json: %w", err)
	}

	req, err = http.NewRequestWithContext(ctx, "PUT", requestURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		panic(fmt.Errorf("failed to build profile update request: %s", err.Error()))
	}

	w.logger.Debug("upserting updated profile", zap.String("id", id))
	resp, err = w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("error executing upsert request: %s", err.Error())
	}

	// Read response body so that http request span is correctly ended
	_, _ = io.ReadAll(resp.Body)
	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("could not close response body: %s", err.Error())
	}

	return nil
}

// Lock a random user profile by setting 'Enabled' to false
func (w userProfileDapi) lockProfile(ctx context.Context, rctx workload.Runctx) error {
	id := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems)))
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", w.connstr, w.bucket, w.scope, w.collection.Name(), id)
	req, err := http.NewRequestWithContext(ctx, "GET", requestURL, nil)
	if err != nil {
		panic(fmt.Errorf("failed to build profile fetch request: %s", err.Error()))
	}

	w.logger.Debug("getting profile to lock", zap.String("id", id))
	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not fetch profile to update: %s", err.Error())
	}

	bodyText, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("could not close response body: %s", err.Error())
	}

	var toUd User
	err = json.Unmarshal(bodyText, &toUd)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyText), err.Error())
	}

	toUd.Enabled = false

	jsonBytes, err := json.Marshal(toUd)
	if err != nil {
		return fmt.Errorf("could not marshal User to json: &s", err.Error())
	}

	req, err = http.NewRequestWithContext(ctx, "PUT", requestURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		panic(fmt.Errorf("failed to build profile update request: %s", err.Error()))
	}

	w.logger.Debug("upserting locked profile", zap.String("id", id))
	resp, err = w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("error executing upsert request: %s", err.Error())
	}

	// Read response body so that http request span is correctly ended
	_, _ = io.ReadAll(resp.Body)
	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("could not close response body: %s", err.Error())
	}

	return nil
}

type DapiQueryPayload struct {
	Statement string `json:"statement"`
}

type DapiUserQueryResponse struct {
	Results []UserQueryResponse `json:"results"`
}

func (w userProfileDapi) findProfile(ctx context.Context, rctx workload.Runctx) error {
	toFind := fmt.Sprintf("%s%%", gofakeit.Letter())
	query := fmt.Sprintf("SELECT * FROM %s.%s.%s WHERE Email LIKE '%s' LIMIT 1", w.bucket, w.scope, w.collection.Name(), toFind)
	payload := DapiQueryPayload{
		Statement: query,
	}
	requestURL := fmt.Sprintf("%s/_p/query/query/service", w.connstr)
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", requestURL, bytes.NewBuffer(body))
	if err != nil {
		panic(fmt.Errorf("failed to build profile fetch request: %s", err.Error()))
	}

	req.Header.Set("Content-Type", "application/json")

	w.logger.Debug("running findProfile query", zap.String("query", query))
	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not execute query request: %s", err.Error())
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("could not close response body: %s", err.Error())
	}

	var results DapiUserQueryResponse
	err = json.Unmarshal(bodyBytes, &results)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyBytes), err.Error())
	}
	return nil
}

type DapiSearchQueryPayload struct {
	Query SearchQuery `json:"query"`
}

type SearchQuery struct {
	Query string `json:"query"`
}

type DapiUserSearchQueryResponse struct {
	Results []SearchQueryResult `json:"hits"`
}

type SearchQueryResult struct {
	Id string `json:"id"`
}

func (w userProfileDapi) findRelatedProfiles(ctx context.Context, rctx workload.Runctx) error {
	interestToFind := Interests[rand.Intn(len(Interests))]

	requestURL := fmt.Sprintf("%s/_p/fts/api/index/interest-index/query", w.connstr)
	payload := DapiSearchQueryPayload{
		Query: SearchQuery{
			Query: interestToFind,
		},
	}

	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", requestURL, bytes.NewBuffer(body))
	if err != nil {
		panic(fmt.Errorf("failed to build profile fetch request: %s", err.Error()))
	}

	req.Header.Set("Content-Type", "application/json")

	w.logger.Debug("performing fts search for profiles with interest", zap.String("interest", interestToFind))
	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not execute search query request: %s", err.Error())
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("could not close response body: %s", err.Error())
	}

	var results DapiUserSearchQueryResponse
	err = json.Unmarshal(bodyBytes, &results)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyBytes), err.Error())
	}

	var matchingUsers []string
	for _, result := range results.Results {
		matchingUsers = append(matchingUsers, result.Id)
	}

	return nil
}
