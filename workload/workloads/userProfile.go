package workloads

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit"
	gotel "github.com/couchbase/gocb-opentelemetry"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/search"
	"github.com/couchbaselabs/spectroperf/workload"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type userProfile struct {
	numItems   int
	bucket     string
	scope      *gocb.Scope
	collection *gocb.Collection
	cluster    *gocb.Cluster
}

func NewUserProfile(numItems int, bucket string, scope *gocb.Scope, collection *gocb.Collection, cluster *gocb.Cluster) userProfile {
	return userProfile{
		numItems:   numItems,
		bucket:     bucket,
		scope:      scope,
		collection: collection,
		cluster:    cluster,
	}
}

type User struct {
	Name      string
	Email     string
	Created   time.Time
	Status    string
	Interests string
	Enabled   bool
}

type UserQueryResponse struct {
	Profiles User
}

var Interests = []string{"Painting", "Drawing", "Sculpting", "Photography", "Writing", "Poetry", "Journaling", "Reading",
	"Gardening", "Hiking", "Cycling", "Running", "Swimming", "Yoga", "Pilates", "Meditation", "Knitting", "Crocheting",
	"Sewing", "Quilting", "Embroidery", "Pottery", "Woodworking", "Baking", "Cooking", "Brewing", "Cake Decorating",
	"DIY Projects", "Calligraphy", "Scrapbooking", "Bird Watching", "Stargazing", "Astronomy", "Fishing", "Hunting",
	"Archery", "Gardening", "Indoor Plants", "Beekeeping", "Bonsai", "Origami", "Magic Tricks", "Playing Guitar",
	"Playing Piano", "Playing Violin", "Playing Drums", "Playing Saxophone", "Singing", "Dancing", "Ballet",
	"Salsa Dancing", "Ballroom Dancing", "Tap Dancing", "Hip-Hop Dance", "Skateboarding", "Snowboarding", "Skiing",
	"Ice Skating", "Rollerblading", "Surfing", "Scuba Diving", "Skydiving", "Rock Climbing", "Bouldering", "Paragliding",
	"Yoga", "Martial Arts", "Kickboxing", "Boxing", "Judo", "Taekwondo", "Karate", "Brazilian Jiu-Jitsu", "Krav Maga",
	"Weightlifting", "Crossfit", "Running Marathons", "Triathlons", "Geocaching", "Volunteering", "Traveling", "Camping",
	"RVing", "Sailing", "Boat Building", "Car Restoration", "Metalworking", "Leatherworking", "Model Building",
	"Lego Building", "Collecting Antiques", "Collecting Stamps", "Collecting Coins", "Collecting Action Figures",
	"Collecting Comics", "Playing Board Games", "Playing Card Games", "Puzzle Solving", "Video Gaming", "VR Gaming",
	"Computer Programming", "Web Design", "Graphic Design", "Animation", "3D Modeling", "Playing Chess", "Playing Mahjong",
	"Playing Poker", "Learning New Languages", "Blogging", "Vlogging", "Podcasting", "Public Speaking", "Debate", "Acting",
	"Film Making", "Voice Acting", "Writing Fiction", "Writing Non-Fiction", "Storytelling", "Community Service",
	"Mentoring", "Networking", "Giving Presentations", "Singing in a Choir", "Playing in a Band", "Circus Arts",
	"Fire Breathing", "Aerial Arts", "Trampoline", "Roller Derby", "Fencing", "Swimming", "Croquet", "Archery", "Curling",
	"Bowling", "Ping Pong", "Tennis", "Badminton", "Golf", "Lawn Darts", "Basketball", "Football", "Baseball", "Soccer",
	"Rugby", "Lacrosse", "Cricket", "Handball"}

// Create a random document with a realistic size from name, email, status text and whether
// or not the account is enabled.
func (w userProfile) GenerateDocument(id string) workload.DocType {
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
		Name: id,
		Data: iu,
	}
}

func (w userProfile) Operations() []string {
	return []string{"fetchProfile", "updateProfile", "lockProfile", "findProfile", "findRelatedProfiles"}
}

func (w userProfile) Probabilities() [][]float64 {
	return [][]float64{
		{0, 0.7, 0.1, 0.15, 0.05},
		{0.8, 0, 0.1, 0.05, 0.05},
		{0.7, 0.2, 0, 0.05, 0.05},
		{0.6, 0.2, 0.15, 0, 0.05},
		{0.6, 0.2, 0.15, 0.05, 0},
	}
}

func (w userProfile) Setup() error {
	gofakeit.Seed(int64(workload.RandSeed))

	err := CreateQueryIndex(w.collection)
	if err != nil {
		return err
	}

	err = EnsureFtsIndex(w.cluster, w.bucket, w.scope.Name(), w.collection.Name())
	if err != nil {
		return err
	}

	return nil
}

func CreateQueryIndex(collection *gocb.Collection) error {
	indexName := "eMailIndex"
	zap.L().Info("Creating query index", zap.String("name", indexName))

	mgr := collection.QueryIndexes()
	err := mgr.CreateIndex(indexName, []string{"Email"}, &gocb.CreateQueryIndexOptions{
		IgnoreIfExists: true,
	})

	if err != nil {
		return errors.Wrap(err, "failed to create eMailIndex")
	}

	return nil
}

func EnsureFtsIndex(cluster *gocb.Cluster, bucket, scope, collection string) error {
	indexName := "interest-index"
	mgr := cluster.SearchIndexes()

	_, err := mgr.GetIndex(indexName, nil)
	if err == nil {
		zap.L().Info("Skipping fts index creation as already present", zap.String("name", indexName))
		return nil
	}

	if !strings.Contains(err.Error(), "index not found") {
		return err
	}

	indexScope := fmt.Sprintf("%s.%s", scope, collection)

	zap.L().Info("Creating fts index", zap.String("name", indexName))
	params := map[string]interface{}{
		"doc_config": map[string]interface{}{
			"mode":       "scope.collection.type_field",
			"type_field": "interests",
		},
		"mapping": map[string]interface{}{
			"default_mapping": map[string]interface{}{
				"dynamic": true,
				"enabled": false,
			},
			"types": map[string]interface{}{
				indexScope: map[string]interface{}{
					"dynamic": true,
					"enabled": true,
				},
			},
		},
	}

	err = mgr.UpsertIndex(gocb.SearchIndex{
		UUID:         "",
		Name:         indexName,
		SourceName:   bucket,
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

	zap.L().Info("Checking fts index is ready to use", zap.String("name", indexName))

	end := time.Now().Add(time.Minute)
	for time.Now().Before(end) {
		_, err = cluster.SearchQuery(
			indexName,
			search.NewMatchQuery("Painting"),
			nil,
		)
		if err != nil {
			time.Sleep(time.Second * 10)
		} else {
			return nil
		}
	}

	return fmt.Errorf("timed out waiting for fts index to be ready: %s", err.Error())
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
	span := trace.SpanFromContext(ctx)
	_, err := w.collection.Get(p, &gocb.GetOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
	if err != nil {
		return fmt.Errorf("profile fetch failed: %s", err.Error())
	}
	rctx.Logger().Sugar().Debugf("fetching profile %s", p)
	return nil
}

// Update the status of a random profile
func (w userProfile) updateProfile(ctx context.Context, rctx workload.Runctx) error {
	p := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems))) // Question to self, should I instead just grab this from context?  probably.
	span := trace.SpanFromContext(ctx)
	result, err := w.collection.Get(p, &gocb.GetOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
	if err != nil {
		return fmt.Errorf("profile fetch during update failed: %s", err.Error())
	}

	var toUd User
	cerr := result.Content(&toUd)
	if cerr != nil {
		return fmt.Errorf("unable to load user into struct: %s", cerr.Error())
	}

	toUd.Status = gofakeit.Paragraph(1, rctx.Rand().Intn(8)+1, rctx.Rand().Intn(12)+1, "\n")

	_, uerr := w.collection.Upsert(p, toUd, &gocb.UpsertOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
	if uerr != nil {
		return fmt.Errorf("data load upsert failed: %s", uerr.Error())
	}
	return nil
}

// Lock a random user profile by setting 'Enabled' to false
func (w userProfile) lockProfile(ctx context.Context, rctx workload.Runctx) error {
	p := fmt.Sprintf("u%d", rctx.Rand().Int31n(int32(w.numItems))) // Question to self, should I instead just grab this from context?  probably.
	span := trace.SpanFromContext(ctx)
	result, err := w.collection.Get(p, &gocb.GetOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
	if err != nil {
		return fmt.Errorf("profile fetch during lock failed: %s", err.Error())
	}

	var toUd User
	result.Content(&toUd)

	toUd.Enabled = false

	_, uerr := w.collection.Upsert(p, toUd, &gocb.UpsertOptions{Context: ctx, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)}) // replace with replace or subdoc
	if uerr != nil {
		return fmt.Errorf("data load upsert failed: %s", uerr.Error())
	}
	return nil
}

// Find a profile using a n1ql query on the email field
func (w userProfile) findProfile(ctx context.Context, rctx workload.Runctx) error {
	toFind := fmt.Sprintf("%s%%", gofakeit.Letter())
	span := trace.SpanFromContext(ctx)

	query := fmt.Sprintf("SELECT * FROM %s WHERE Email LIKE $email LIMIT 1", w.collection.Name())
	rctx.Logger().Sugar().Debugf("Querying with %s using param %s", query, toFind)
	params := make(map[string]interface{}, 1)
	params["email"] = toFind

	rows, err := w.scope.Query(query, &gocb.QueryOptions{NamedParameters: params, Adhoc: true, ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span)})
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
	interestToFind := Interests[rand.Intn(len(Interests))]
	span := trace.SpanFromContext(ctx)

	rctx.Logger().Sugar().Debugf("Finding profiles that contain the interest %s", interestToFind)

	matchResult, err := w.cluster.SearchQuery(
		"interest-index",
		search.NewMatchQuery(interestToFind),
		&gocb.SearchOptions{
			ParentSpan: gotel.NewOpenTelemetryRequestSpan(ctx, span),
		},
	)

	if err != nil {
		return fmt.Errorf("fts query failed: %s", err.Error())
	}

	var matchingUsers []string
	for matchResult.Next() {
		row := matchResult.Row()
		matchingUsers = append(matchingUsers, row.ID)
	}

	rctx.Logger().Sugar().Debugf("Found users interested in %s: %v\n", interestToFind, matchingUsers)

	err = matchResult.Err()
	if err != nil {
		return fmt.Errorf("error iterating the rows: %s", err.Error())
	}

	return nil
}
