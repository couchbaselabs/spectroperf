package configuration

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const defaultNumItems = 500

// NewFlagSet returns a flag set configured with all spectroperf configuration flags.
func NewFlagSet() *pflag.FlagSet {
	flagSet := pflag.NewFlagSet("spectroperf-config", pflag.ContinueOnError)

	flagSet.String("connstr", "", "connection string of the cluster under test")
	flagSet.String("dapi-connstr", "", "connection string for data api")
	flagSet.String("username", DefaultUsername, "username for cluster under test")
	flagSet.String("password", DefaultPassword, "password of the cluster under test")
	flagSet.String("cert", "", "path to certificate file")
	flagSet.Bool("tls-skip-verify", false, "skip tls certificate verification")
	flagSet.String("log-level", DefaultLogLevel, "the log level to run at")
	flagSet.String("workload", "", "workload name")
	flagSet.Int("num-items", defaultNumItems, "number of docs to create")
	flagSet.IntSlice("num-users", []int{}, "number of concurrent simulated users; single value or comma-separated list for stepped runs")
	flagSet.String("run-time", DefaultRunTime, "total time to run the workload (e.g. '5m', '30s')")
	flagSet.String("ramp-time", DefaultRampTime, "length of ramp-up and ramp-down periods (e.g. '1m', '30s')")
	flagSet.String("only-operation", "", "the only operation to run from the workload")
	flagSet.String("sleep", "", "time to sleep between operations")
	flagSet.String("bucket", DefaultBucket, "bucket name")
	flagSet.String("scope", DefaultScope, "scope name")
	flagSet.String("collection", DefaultCollection, "collection name")
	flagSet.Bool("enable-tracing", false, "enables otel tracing")
	flagSet.String("otlp-endpoint", DefaultOtlpEndpoint, "endpoint otel traces will be exported to")
	flagSet.String("otel-exporter-headers", "", "comma separated list of otel exporter headers, e.g 'header1=value1,header2=value2'")
	flagSet.Int("dial-timeout", DefaultDialTimeout, "TCP dial timeout in seconds for DAPI HTTP clients")
	flagSet.Int("response-header-timeout", DefaultResponseHeaderTimeout, "response header timeout in seconds for DAPI HTTP clients")
	flagSet.Int("request-timeout", DefaultRequestTimeout, "overall request timeout in seconds for DAPI HTTP clients")
	flagSet.Int("idle-conn-timeout", DefaultIdleConnTimeout, "idle connection timeout in seconds for DAPI HTTP clients")

	return flagSet
}

// BindFlagSet binds the provided flag set to Viper so defaults are available through ReadConfig.
func BindFlagSet(flagSet *pflag.FlagSet) error {
	return viper.BindPFlags(flagSet)
}
