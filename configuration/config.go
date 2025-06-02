package configuration

import "github.com/couchbaselabs/spectroperf/workload"

// Config represents all the configuration settings for spectroperf.
type Config struct {
	Connstr             string
	Cert                string
	Username            string
	Password            string
	Bucket              string
	Scope               string
	Collection          string
	NumItems            int
	NumUsers            int
	ConfigFile          string
	TlsSkipVerify       bool
	Workload            string
	DapiConnstr         string
	RunTime             int
	RampTime            int
	OtlpEndpoint        string
	EnableTracing       bool
	OtelExporterHeaders string
	Debug               bool
	MarkovChain         [][]float64
	OnlyOperation       string
	Sleep               string
}

// DefaultConfig returns a config intialised with sensible default values
func DefaultConfig() Config {
	return Config{
		Connstr:             "",
		Cert:                "",
		Username:            "Administrator",
		Password:            "password",
		Bucket:              "data",
		Scope:               "identity",
		Collection:          "profiles",
		NumItems:            500,
		NumUsers:            500,
		TlsSkipVerify:       false,
		Workload:            "",
		DapiConnstr:         "",
		RunTime:             5,
		RampTime:            1,
		OtlpEndpoint:        workload.DefaultOtlpEndpoint,
		EnableTracing:       false,
		OtelExporterHeaders: "",
		Debug:               false,
		MarkovChain:         [][]float64{},
		OnlyOperation:       "",
	}
}
