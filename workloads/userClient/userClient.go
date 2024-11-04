package userClient

import (
	"context"
	"time"
)

type User struct {
	Name    string
	Email   string
	Created time.Time
	Status  string
	Enabled bool
}

type UserQueryResponse struct {
	Profiles User `json:"profiles"`
}

type UserClient interface {
	GetUser(ctx context.Context, id string) (*User, error)
	UpsertUser(ctx context.Context, id string, user User) error
	FindUser(ctx context.Context, query string) (*User, error)
}
