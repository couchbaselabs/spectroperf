package workloads

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/brianvoe/gofakeit"
	"github.com/couchbaselabs/spectroperf/workload"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"
)

type userProfileDapi struct {
	connstr    string
	username   string
	password   string
	client     *http.Client
	numItems   int
	bucket     string
	scope      string
	collection string
}

func NewUserProfileDapi(connstr string, bucket string, scope string, collection string, numItems int, usr string, pwd string) userProfileDapi {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		MaxConnsPerHost: 500,
	}
	return userProfileDapi{
		connstr:    connstr,
		username:   usr,
		password:   pwd,
		client:     &http.Client{Transport: tr},
		numItems:   numItems,
		bucket:     bucket,
		scope:      scope,
		collection: collection,
	}
}

// Create a random document with a realistic size from name, email, status text and whether
// or not the account is enabled.
func (w userProfileDapi) GenerateDocument(id string) workload.DocType {
	rng := rand.NewSource(int64(workload.RandSeed))
	r := rand.New(rng)

	iu := User{
		Name:    gofakeit.Name(),
		Email:   gofakeit.Email(), // TODO: make the email actually based on the name (pedantic)
		Created: gofakeit.DateRange(time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)),
		Status:  gofakeit.Paragraph(1, r.Intn(8)+1, r.Intn(12)+1, "\n"),
		Enabled: true,
	}

	return workload.DocType{
		Name: id,
		Data: iu,
	}
}

func (w userProfileDapi) Operations() []string {
	return []string{"fetchProfile", "updateProfile", "lockProfile", "findProfile", "findRelatedProfiles"}
}

func (w userProfileDapi) Probabilities() [][]float64 {
	return [][]float64{
		{0, 0.8, 0.1, 0.05, 0.05},
		{0.6, 0, 0.2, 0.1, 0.1},
		{0.5, 0.3, 0, 0.15, 0.05},
		{0.4, 0.3, 0.2, 0, 0.1},
		{0.3, 0.3, 0.2, 0.2, 0},
	}
}

func (w userProfileDapi) Setup() error {
	// TODO setup FTS index here for findRelatedProfile
	gofakeit.Seed(int64(workload.RandSeed))
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
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", w.connstr, w.bucket, w.scope, w.collection, id)
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build profile fetch request: %s", err.Error())
	}

	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not fetch profile to update: %s", err.Error())
	}

	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
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
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", w.connstr, w.bucket, w.scope, w.collection, id)
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build profile fetch request: %s", err.Error())
	}

	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not fetch profile to update: %s", err.Error())
	}

	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
	}

	var toUd User
	err = json.Unmarshal(bodyText, &toUd)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyText), err.Error())
	}

	toUd.Status = gofakeit.Paragraph(1, rctx.Rand().Intn(8)+1, rctx.Rand().Intn(12)+1, "\n")

	jsonBytes, err := json.Marshal(toUd)
	if err != nil {
		return fmt.Errorf("could not marshal User to json: &s", err.Error())
	}

	req, err = http.NewRequest("PUT", requestURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to build profile update request: %s", err.Error())
	}

	_, err = w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("error executing upsert request: %s", err.Error())
	}
	return nil
}

// Lock a random user profile by setting 'Enabled' to false
func (w userProfileDapi) lockProfile(ctx context.Context, rctx workload.Runctx) error {
	id := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems)))
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", w.connstr, w.bucket, w.scope, w.collection, id)
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build profile fetch request: %s", err.Error())
	}

	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not fetch profile to update: %s", err.Error())
	}

	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
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

	req, err = http.NewRequest("PUT", requestURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to build profile update request: %s", err.Error())
	}

	_, err = w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("error executing upsert request: %s", err.Error())
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
	query := fmt.Sprintf("SELECT * FROM %s.%s.%s WHERE Email LIKE '%s' LIMIT 1", w.bucket, w.scope, w.collection, toFind)
	payload := DapiQueryPayload{
		Statement: query,
	}
	requestURL := fmt.Sprintf("%s/_p/query/query/service", w.connstr)
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", requestURL, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to build profile fetch request: %s", err.Error())
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := w.executeRequest(req)
	if err != nil {
		return fmt.Errorf("could not execute query request: %s", err.Error())
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %s", err.Error())
	}

	var results DapiUserQueryResponse
	err = json.Unmarshal(bodyBytes, &results)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyBytes), err.Error())
	}
	return nil
}

func (w userProfileDapi) findRelatedProfiles(ctx context.Context, rctx workload.Runctx) error {
	return nil
}
