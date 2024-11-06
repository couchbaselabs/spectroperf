package userClient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

type DapiUserQueryResponse struct {
	Results []UserQueryResponse `json:"results"`
}

type DapiQueryPayload struct {
	Statement string `json:"statement"`
}

type dapiClient struct {
	connstr    string
	bucket     string
	scope      string
	collection string
	username   string
	password   string
	client     *http.Client
}

func NewDapiClient(connstr string, bucket string, scope string, collection string, username string, password string) *dapiClient {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &dapiClient{
		connstr:    connstr,
		bucket:     bucket,
		scope:      scope,
		collection: collection,
		username:   username,
		password:   password,
		client:     &http.Client{Transport: tr},
	}
}

func (c *dapiClient) executeRequest(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(c.username, c.password)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute get request: %s", err.Error())
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("profile fetch returned unexpected status code %d", resp.StatusCode)
	}
	return resp, nil
}

func (c *dapiClient) GetUser(ctx context.Context, id string) (*User, error) {
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", c.connstr, c.bucket, c.scope, c.collection, id)
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build profile fetch request: %s", err.Error())
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("could not fetch profile to update: %s", err.Error())
	}

	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read response body: %s", err.Error())
	}

	var toUd User
	err = json.Unmarshal(bodyText, &toUd)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyText), err.Error())
	}
	return &toUd, nil
}

func (c *dapiClient) UpsertUser(ctx context.Context, id string, user User) error {
	requestURL := fmt.Sprintf("%s/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", c.connstr, c.bucket, c.scope, c.collection, id)

	jsonBytes, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("could not marshal User to json: &s", err.Error())
	}

	req, err := http.NewRequest("PUT", requestURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to build profile update request: %s", err.Error())
	}

	_, err = c.executeRequest(req)
	if err != nil {
		return fmt.Errorf("error executing upsert request: %s", err.Error())
	}

	return nil
}

func (c *dapiClient) FindUser(ctx context.Context, query string) (*User, error) {
	payload := DapiQueryPayload{
		Statement: query,
	}
	requestURL := fmt.Sprintf("%s/_p/query/query/service", c.connstr)
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", requestURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to build profile fetch request: %s", err.Error())
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := c.executeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("could not execute query request: %s", err.Error())
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read response body: %s", err.Error())
	}

	var results DapiUserQueryResponse
	err = json.Unmarshal(bodyBytes, &results)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal response body - %s : %s", string(bodyBytes), err.Error())
	}

	if len(results.Results) > 0 {
		return &results.Results[0].Profiles, nil
	}
	return nil, fmt.Errorf("could not find user")
}
