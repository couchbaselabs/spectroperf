package workloads

import (
	"fmt"
	"sort"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/configuration"
	"github.com/couchbaselabs/spectroperf/workload"
	"go.uber.org/zap"
)

// Constructor builds a Workload from the parsed config and a connected cluster.
type Constructor func(logger *zap.Logger, config *configuration.Config, cluster *gocb.Cluster) workload.Workload

// registry maps the name used in the --workload flag to the constructor that builds it.
// A new workload only becomes selectable once it is added here, and every entry is
// covered by the conformance test in conformance_test.go.
var registry = map[string]Constructor{
	"basic":             NewBasic,
	"basic-dapi":        NewBasicDapi,
	"user-profile":      NewUserProfile,
	"user-profile-dapi": NewUserProfileDapi,
}

// Validate reports whether a workload is registered under the given name. It needs no
// cluster, so the caller can reject a bad --workload before anything is dialled.
func Validate(name string) error {
	if name == "" {
		return fmt.Errorf("no workload specified, expected one of %v", Names())
	}

	if _, ok := registry[name]; !ok {
		return fmt.Errorf("unknown workload %q, expected one of %v", name, Names())
	}

	return nil
}

// New builds the named workload, returning an error if no workload is registered under that name.
func New(name string, logger *zap.Logger, config *configuration.Config, cluster *gocb.Cluster) (workload.Workload, error) {
	if err := Validate(name); err != nil {
		return nil, err
	}

	return registry[name](logger, config, cluster), nil
}

// Names returns the registered workload names in alphabetical order.
func Names() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}
