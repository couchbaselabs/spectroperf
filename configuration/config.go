package configuration

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Config represents all the configuration settings for spectroperf.
type Config struct {
	Connstr       string
	DapiConnstr   string
	Username      string
	Password      string
	Cert          string
	TlsSkipVerify bool
	LogLevel      string

	Bucket     string
	Scope      string
	Collection string
	NumItems   int
	NumUsers   int
	ConfigFile string

	Workload string

	RunTime             int
	RampTime            int
	OtlpEndpoint        string
	EnableTracing       bool
	OtelExporterHeaders string
	MarkovChain         [][]float64
	OnlyOperation       string
	Sleep               string
}

func ReadConfig(logger *zap.Logger) *Config {
	var markovChain [][]float64
	err := viper.UnmarshalKey("markov-chain", &markovChain)
	if err != nil {
		logger.Fatal("unmarshalling markov chain from config file")
	}

	config := &Config{
		Connstr:             viper.GetString("connstr"),
		DapiConnstr:         viper.GetString("dapi-connstr"),
		Username:            viper.GetString("username"),
		Password:            viper.GetString("password"),
		Cert:                viper.GetString("cert"),
		TlsSkipVerify:       viper.GetBool("tls-skip-verify"),
		LogLevel:            viper.GetString("log-level"),
		Workload:            viper.GetString("workload"),
		NumItems:            viper.GetInt("num-items"),
		NumUsers:            viper.GetInt("num-users"),
		RunTime:             viper.GetInt("run-time"),
		RampTime:            viper.GetInt("ramp-time"),
		OnlyOperation:       viper.GetString("only-operation"),
		Sleep:               viper.GetString("sleep"),
		Bucket:              viper.GetString("bucket"),
		Scope:               viper.GetString("scope"),
		Collection:          viper.GetString("collection"),
		EnableTracing:       viper.GetBool("enable-tracing"),
		OtlpEndpoint:        viper.GetString("otlp-endpoint"),
		OtelExporterHeaders: viper.GetString("otel-exporter-headers"),
		MarkovChain:         markovChain,
	}

	logger.Info("parsed configuration", zap.Any("config", config))

	return config
}
