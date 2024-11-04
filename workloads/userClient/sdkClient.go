package userClient

import (
	"context"
	"fmt"
	"github.com/couchbase/gocb/v2"
)

type sdkClient struct {
	scope      gocb.Scope
	collection gocb.Collection
}

func NewSdkClient(scope gocb.Scope, collection gocb.Collection) *sdkClient {
	return &sdkClient{
		scope:      scope,
		collection: collection,
	}
}

func (c *sdkClient) GetUser(ctx context.Context, id string) (*User, error) {
	result, err := c.collection.Get(id, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("profile fetch failed: %s", err.Error())
	}

	var toUd User
	err = result.Content(&toUd)
	if err != nil {
		return nil, fmt.Errorf("unable to load user into struct: %s", err.Error())
	}

	return &toUd, nil
}

func (c *sdkClient) UpsertUser(ctx context.Context, id string, user User) error {
	_, err := c.collection.Upsert(id, user, nil)
	if err != nil {
		return fmt.Errorf("data load upsert failed: %s", err.Error())
	}
	return nil
}

func (c *sdkClient) FindUser(ctx context.Context, query string) (*User, error) {
	rows, err := c.scope.Query(query, &gocb.QueryOptions{Adhoc: true})
	if err != nil {
		return nil, fmt.Errorf("query failed: %s", err.Error())
	}

	var resp UserQueryResponse
	for rows.Next() {
		err := rows.Row(&resp)
		if err != nil {
			return nil, fmt.Errorf("could not read next row: %s", err.Error())
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating the rows: %s", err.Error())
	}
	return &resp.Profiles, nil
}
