package configuration

import (
	"fmt"
	"os"
	"slices"

	"github.com/BurntSushi/toml"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	DefaultUsername = "Administrator"
	DefaultPassword = "password"

	DefaultLogLevel = "info"

	DefaultBucket     = "data"
	DefaultScope      = "identity"
	DefaultCollection = "profiles"

	DefaultOtlpEndpoint = "localhost:4318"

	DefaultRunTime  = "5m"
	DefaultRampTime = "0m"

	DefaultDialTimeout           = 10
	DefaultResponseHeaderTimeout = 30
	DefaultRequestTimeout        = 60
	DefaultIdleConnTimeout       = 30
)

// Config represents all the configuration settings for spectroperf.
type Config struct {
	Connstr       string `toml:"connstr,omitempty"`
	DapiConnstr   string `toml:"dapi-connstr,omitempty"`
	Username      string `toml:"username,omitempty"`
	Password      string `toml:"password,omitempty"`
	Cert          string `toml:"cert,omitempty"`
	TlsSkipVerify bool   `toml:"tls-skip-verify,omitempty"`
	LogLevel      string `toml:"log-level,omitempty"`

	Bucket     string `toml:"bucket,omitempty"`
	Scope      string `toml:"scope,omitempty"`
	Collection string `toml:"collection,omitempty"`
	NumItems   int    `toml:"num-items,omitempty"`
	NumUsers   int    `toml:"num-users,omitempty"`
	ConfigFile string `toml:"config-file,omitempty"`

	Workload string `toml:"workload"`

	RunTime             string      `toml:"run-time,omitempty"`
	RampTime            string      `toml:"ramp-time,omitempty"`
	OtlpEndpoint        string      `toml:"otlp-endpoint,omitempty"`
	EnableTracing       bool        `toml:"enable-tracing,omitempty"`
	OtelExporterHeaders string      `toml:"otel-exported-headers,omitempty"`
	MarkovChain         [][]float64 `toml:"markov-chain"`
	OnlyOperation       string      `toml:"only-operation,omitempty"`
	Sleep               string      `toml:"sleep,omitempty"`

	DialTimeout           int `toml:"dial-timeout,omitempty"`
	ResponseHeaderTimeout int `toml:"response-header-timeout,omitempty"`
	RequestTimeout        int `toml:"request-timeout,omitempty"`
	IdleConnTimeout       int `toml:"idle-conn-timeout,omitempty"`
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
		RunTime:             viper.GetString("run-time"),
		RampTime:            viper.GetString("ramp-time"),
		OnlyOperation:       viper.GetString("only-operation"),
		Sleep:               viper.GetString("sleep"),
		Bucket:              viper.GetString("bucket"),
		Scope:               viper.GetString("scope"),
		Collection:          viper.GetString("collection"),
		EnableTracing:       viper.GetBool("enable-tracing"),
		OtlpEndpoint:        viper.GetString("otlp-endpoint"),
		OtelExporterHeaders: viper.GetString("otel-exporter-headers"),
		MarkovChain:         markovChain,

		DialTimeout:           viper.GetInt("dial-timeout"),
		ResponseHeaderTimeout: viper.GetInt("response-header-timeout"),
		RequestTimeout:        viper.GetInt("request-timeout"),
		IdleConnTimeout:       viper.GetInt("idle-conn-timeout"),
	}

	logger.Info("parsed configuration", zap.Any("config", config))

	return config
}

func WriteConfig(config *Config, timeStamp string, defaultMarkov [][]float64) error {
	filePath := fmt.Sprintf("%s/config.toml", timeStamp)

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := toml.NewEncoder(f)

	clearDefaults(config, defaultMarkov)
	return encoder.Encode(config)
}

func clearDefaults(config *Config, defaultMarkov [][]float64) {
	if config.RunTime == DefaultRunTime {
		config.RunTime = ""
	}

	if config.RampTime == DefaultRampTime {
		config.RampTime = ""
	}

	if config.Username == DefaultUsername {
		config.Username = ""
	}

	if config.Password == DefaultPassword {
		config.Password = ""
	}

	if config.LogLevel == DefaultLogLevel {
		config.LogLevel = ""
	}

	if config.Bucket == DefaultBucket {
		config.Bucket = ""
	}

	if config.Scope == DefaultScope {
		config.Scope = ""
	}

	if config.Collection == DefaultCollection {
		config.Collection = ""
	}

	if config.OtlpEndpoint == DefaultOtlpEndpoint {
		config.OtlpEndpoint = ""
	}

	markovChainDefault := true
	for i, row := range config.MarkovChain {
		if !slices.Equal(row, defaultMarkov[i]) {
			markovChainDefault = false
		}
	}

	if markovChainDefault {
		config.MarkovChain = nil
	}
}
