package configuration

import (
	"flag"
	"strconv"
)

// parseFlags reads the flags from the command line into a Config struct.
func ParseFlags() Config {
	flags := Config{}
	flag.StringVar(&flags.Connstr, "connstr", "", "connection string of the cluster under test")
	flag.StringVar(&flags.Cert, "cert", "", "path to certificate file")
	flag.StringVar(&flags.Username, "username", "Administrator", "username for cluster under test")
	flag.StringVar(&flags.Password, "password", "password", "password of the cluster under test")
	flag.StringVar(&flags.Bucket, "bucket", "data", "bucket name")
	flag.StringVar(&flags.Scope, "scope", "identity", "scope name")
	flag.StringVar(&flags.Collection, "collection", "profiles", "collection name")
	flag.IntVar(&flags.NumItems, "num-items", 0, "number of docs to create")
	flag.IntVar(&flags.NumUsers, "num-users", 0, "number of concurrent simulated users accessing the data")
	flag.BoolVar(&flags.TlsSkipVerify, "tls-skip-verify", false, "skip TLS certificate verification")
	flag.StringVar(&flags.Workload, "workload", "", "workload name")
	flag.StringVar(&flags.DapiConnstr, "dapi-connstr", "", "connection string for data api")
	flag.IntVar(&flags.RunTime, "run-time", 0, "total time to run the workload in minutes")
	flag.IntVar(&flags.RampTime, "ramp-time", 0, "length of ramp-up and ramp-down periods in minutes")
	flag.StringVar(&flags.ConfigFile, "config-file", "", "path to configuration file")
	flag.StringVar(&flags.OtlpEndpoint, "otlp-endpoint", "", "endpoint OTEL traces will be exported to")
	flag.BoolVar(&flags.EnableTracing, "enable-tracing", false, "enables OTEL tracing")
	flag.StringVar(&flags.OtelExporterHeaders, "otel-exporter-headers", "", "a comma seperated list of otlp expoter headers, e.g 'header1=value1,header2=value2'")
	flag.BoolVar(&flags.Debug, "debug", false, "turn off debug level logging")
	flag.StringVar(&flags.OnlyOperation, "only-operation", "", "the only operation to run from the workload")
	flag.StringVar(&flags.Sleep, "sleep", "", "time to sleep between operations")
	flag.Parse()

	return flags
}

// OverwriteConfigWithFlags takes a config and overwrites any relevant fields
// with the given flag value if it has been set.
func OverwriteConfigWithFlags(c Config) Config {
	overwrite := func(flag *flag.Flag) {
		switch flag.Name {
		case "connstr":
			c.Connstr = flag.Value.String()
		case "cert":
			c.Cert = flag.Value.String()
		case "username":
			c.Username = flag.Value.String()
		case "password":
			c.Password = flag.Value.String()
		case "bucket":
			c.Bucket = flag.Value.String()
		case "scope":
			c.Scope = flag.Value.String()
		case "collection":
			c.Collection = flag.Value.String()
		case "num-items":
			number, _ := strconv.Atoi(flag.Value.String())
			c.NumItems = number
		case "num-users":
			number, _ := strconv.Atoi(flag.Value.String())
			c.NumUsers = number
		case "tls-skip-verify":
			boolValue, _ := strconv.ParseBool(flag.Value.String())
			c.TlsSkipVerify = boolValue
		case "workload":
			c.Workload = flag.Value.String()
		case "dapi-connstr":
			c.DapiConnstr = flag.Value.String()
		case "run-time":
			number, _ := strconv.Atoi(flag.Value.String())
			c.RunTime = number
		case "ramp-time":
			number, _ := strconv.Atoi(flag.Value.String())
			c.RampTime = number
		case "otlp-endpoint":
			c.OtlpEndpoint = flag.Value.String()
		case "enable-tracing":
			boolValue, _ := strconv.ParseBool(flag.Value.String())
			c.EnableTracing = boolValue
		case "otel-exporter-headers":
			c.OtelExporterHeaders = flag.Value.String()
		case "debug":
			boolValue, _ := strconv.ParseBool(flag.Value.String())
			c.Debug = boolValue
		case "only-operation":
			c.OnlyOperation = flag.Value.String()
		}
	}

	flag.Visit(overwrite)
	return c
}
