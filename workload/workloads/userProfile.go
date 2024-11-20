package workloads

import (
	"context"
	"fmt"
	"github.com/brianvoe/gofakeit"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/workload"
	"math/rand"
	"time"
)

type userProfile struct {
	numItems   int
	scope      *gocb.Scope
	collection *gocb.Collection
}

func NewUserProfile(numItems int, scope *gocb.Scope, collection *gocb.Collection) userProfile {
	return userProfile{
		numItems:   numItems,
		scope:      scope,
		collection: collection,
	}
}

type User struct {
	Name    string
	Email   string
	Created time.Time
	Status  string
	Enabled bool
}

type UserQueryResponse struct {
	Profiles User
}

// Create a random document with a realistic size from name, email, status text and whether
// or not the account is enabled.
func (w userProfile) GenerateDocument(id string) workload.DocType {
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

func (w userProfile) Operations() []string {
	return []string{"fetchProfile", "updateProfile", "lockProfile", "findProfile", "findRelatedProfiles"}
}

func (w userProfile) Probabilities() [][]float64 {
	return [][]float64{
		{0, 0.8, 0.1, 0.05, 0.05},
		{0.6, 0, 0.2, 0.1, 0.1},
		{0.5, 0.3, 0, 0.15, 0.05},
		{0.4, 0.3, 0.2, 0, 0.1},
		{0.3, 0.3, 0.2, 0.2, 0},
	}
}

func (w userProfile) Setup() error {
	// TODO setup FTS index here for findRelatedProfile
	gofakeit.Seed(int64(workload.RandSeed))
	return nil
}

func (w userProfile) Functions() map[string]func(ctx context.Context, rctx workload.Runctx) error {
	return map[string]func(ctx context.Context, rctx workload.Runctx) error{
		"fetchProfile":        w.fetchProfile,        // similar to login or looking at someone
		"updateProfile":       w.updateProfile,       // updating a status on the profile
		"lockProfile":         w.lockProfile,         // disable or enable a random profile (account lockout)
		"findProfile":         w.findProfile,         // find a profile by a secondary index (email address)
		"findRelatedProfiles": w.findRelatedProfiles, // look for people with similar interests
	}
}

// Fetch a random profile in the range of profiles
func (w userProfile) fetchProfile(ctx context.Context, rctx workload.Runctx) error {
	p := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems)))
	_, err := w.collection.Get(p, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("profile fetch failed: %s", err.Error())
	}
	rctx.Logger().Sugar().Debugf("fetching profile %s", p)
	return nil
}

// Update the status of a random profile
func (w userProfile) updateProfile(ctx context.Context, rctx workload.Runctx) error {
	p := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems))) // Question to self, should I instead just grab this from context?  probably.
	result, err := w.collection.Get(p, nil)
	if err != nil {
		return fmt.Errorf("profile fetch during update failed: %s", err.Error())
	}

	var toUd User
	cerr := result.Content(&toUd)
	if cerr != nil {
		return fmt.Errorf("unable to load user into struct: %s", cerr.Error())
	}

	toUd.Status = gofakeit.Paragraph(1, rctx.Rand().Intn(8)+1, rctx.Rand().Intn(12)+1, "\n")

	_, uerr := w.collection.Upsert(p, toUd, nil)
	if uerr != nil {
		return fmt.Errorf("data load upsert failed: %s", uerr.Error())
	}
	return nil
}

// Lock a random user profile by setting 'Enabled' to false
func (w userProfile) lockProfile(ctx context.Context, rctx workload.Runctx) error {
	p := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems))) // Question to self, should I instead just grab this from context?  probably.
	result, err := w.collection.Get(p, nil)
	if err != nil {
		return fmt.Errorf("profile fetch during lock failed: %s", err.Error())
	}

	var toUd User
	result.Content(&toUd)

	toUd.Enabled = false

	_, uerr := w.collection.Upsert(p, toUd, nil) // replace with replace or subdoc
	if uerr != nil {
		return fmt.Errorf("data load upsert failed: %s", uerr.Error())
	}
	return nil
}

// Find a profile using a n1ql query on the email field
func (w userProfile) findProfile(ctx context.Context, rctx workload.Runctx) error {
	toFind := fmt.Sprintf("%s%%", gofakeit.Letter())

	query := "SELECT * FROM profiles WHERE Email LIKE $email LIMIT 1"
	rctx.Logger().Sugar().Debugf("Querying with %s using param %s", query, toFind)
	params := make(map[string]interface{}, 1)
	params["email"] = toFind

	rows, err := w.scope.Query(query, &gocb.QueryOptions{NamedParameters: params, Adhoc: true})
	if err != nil {
		return fmt.Errorf("query failed: %s", err.Error())
	}

	for rows.Next() {
		var resp UserQueryResponse
		err := rows.Row(&resp)
		if err != nil {
			return fmt.Errorf("could not read next row: %s", err.Error())
		}
		rctx.Logger().Sugar().Debugf("Found a User: %+v", resp.Profiles)
	}

	err = rows.Err()
	if err != nil {
		return fmt.Errorf("error iterating the rows: %s", err.Error())
	}
	return nil
}

func (w userProfile) findRelatedProfiles(ctx context.Context, rctx workload.Runctx) error {
	return nil

	// toFind := gofakeit.Paragraph(1, 1, ctx.r.Intn(12)+1, "\n") // one sentence to search

	// ctx.l.Sugar().Debugf("Searching for related profiles with string %s", toFind)
	// params := make(map[string]interface{}, 1)
	// params["email"] = toFind

	// matchResult, err := scope.Search(
	// 	"profile-statuses",
	// 	search.NewMatchQuery(tofind),
	// 	&gocb.SearchOptions{
	// 		Limit:  10,
	// 		Fields: []string{"description"},
	// 	},
	// )
	// if err != nil {
	// 	panic(err)
	// }

	// for matchResult.Next() {
	// 	row := matchResult.Row()
	// 	docID := row.ID
	// 	score := row.Score

	// 	var fields interface{}
	// 	err := row.Fields(&fields)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	fmt.Printf("Document ID: %s, search score: %f, fields included in result: %v\n", docID, score, fields)
	// }

	// // always check for errors after iterating
	// err = matchResult.Err()
	// if err != nil {
	// 	panic(err)
	// }

}