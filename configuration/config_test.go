package configuration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestReadConfigFromFlags(t *testing.T) {
	resetViperConfig(t)

	expected := &Config{
		Connstr:               "couchbase://localhost",
		DapiConnstr:           "http://dapi",
		Username:              "user",
		Password:              "pass",
		Cert:                  "cert-path",
		TlsSkipVerify:         true,
		LogLevel:              "debug",
		Workload:              "test-workload",
		NumItems:              123,
		NumUsers:              []int{4, 5, 6},
		RunTime:               "10m",
		RampTime:              "1m",
		OnlyOperation:         "op",
		Sleep:                 "200ms",
		Bucket:                "b",
		Scope:                 "s",
		Collection:            "c",
		EnableTracing:         true,
		OtlpEndpoint:          "otel:4318",
		OtelExporterHeaders:   "key=value",
		DialTimeout:           11,
		ResponseHeaderTimeout: 22,
		RequestTimeout:        33,
		IdleConnTimeout:       44,
		MarkovChain:           [][]float64{{0.5, 0.5}, {0.3, 0.7}},
	}

	viper.Set("connstr", expected.Connstr)
	viper.Set("dapi-connstr", expected.DapiConnstr)
	viper.Set("username", expected.Username)
	viper.Set("password", expected.Password)
	viper.Set("cert", expected.Cert)
	viper.Set("tls-skip-verify", expected.TlsSkipVerify)
	viper.Set("log-level", expected.LogLevel)
	viper.Set("workload", expected.Workload)
	viper.Set("num-items", expected.NumItems)
	viper.Set("num-users", expected.NumUsers)
	viper.Set("run-time", expected.RunTime)
	viper.Set("ramp-time", expected.RampTime)
	viper.Set("only-operation", expected.OnlyOperation)
	viper.Set("sleep", expected.Sleep)
	viper.Set("bucket", expected.Bucket)
	viper.Set("scope", expected.Scope)
	viper.Set("collection", expected.Collection)
	viper.Set("enable-tracing", expected.EnableTracing)
	viper.Set("otlp-endpoint", expected.OtlpEndpoint)
	viper.Set("otel-exporter-headers", expected.OtelExporterHeaders)
	viper.Set("dial-timeout", expected.DialTimeout)
	viper.Set("response-header-timeout", expected.ResponseHeaderTimeout)
	viper.Set("request-timeout", expected.RequestTimeout)
	viper.Set("idle-conn-timeout", expected.IdleConnTimeout)
	viper.Set("markov-chain", expected.MarkovChain)

	logger := zaptest.NewLogger(t)
	cfg := ReadConfig(logger)

	assert.Equal(t, expected, cfg)

	resetViperConfig(t)

	// Test that num-users correctly parsed when set as single int instead of slice
	numUsers := 789
	viper.Set("num-users", numUsers)
	cfg = ReadConfig(logger)
	assert.Equal(t, []int{numUsers}, cfg.NumUsers)
}

func TestReadConfigFromToml(t *testing.T) {
	expected := &Config{
		Connstr:               "couchbase://localhost",
		DapiConnstr:           "http://dapi",
		Username:              "user",
		Password:              "pass",
		Cert:                  "cert-path",
		TlsSkipVerify:         true,
		LogLevel:              "debug",
		Workload:              "test-workload",
		NumItems:              123,
		NumUsers:              []int{4, 5, 6},
		RunTime:               "10m",
		RampTime:              "1m",
		OnlyOperation:         "op",
		Sleep:                 "200ms",
		Bucket:                "b",
		Scope:                 "s",
		Collection:            "c",
		EnableTracing:         true,
		OtlpEndpoint:          "otel:4318",
		OtelExporterHeaders:   "key=value",
		DialTimeout:           11,
		ResponseHeaderTimeout: 22,
		RequestTimeout:        33,
		IdleConnTimeout:       44,
		MarkovChain:           [][]float64{{0.5, 0.5}, {0.3, 0.7}},
	}

	tomlConfig := `
connstr = "couchbase://localhost"
dapi-connstr = "http://dapi"
username = "user"
password = "pass"
cert = "cert-path" 
tls-skip-verify = true
log-level = "debug"
workload = "test-workload"
num-items = 123
num-users = [4, 5, 6]
run-time = "10m"
ramp-time = "1m"
only-operation = "op"
sleep = "200ms"
bucket = "b"
scope = "s"
collection = "c"
enable-tracing = true
otlp-endpoint = "otel:4318"
otel-exporter-headers = "key=value"
dial-timeout = 11
response-header-timeout = 22
request-timeout = 33
idle-conn-timeout = 44
markov-chain = [[0.5, 0.5], [0.3, 0.7]]
`

	setViperConfigFromToml(t, tomlConfig)
	logger := zaptest.NewLogger(t)
	cfg := ReadConfig(logger)

	assert.Equal(t, expected, cfg)

	resetViperConfig(t)

	// Test that num-users correctly parsed when set as single int instead of slice
	numUsers := 789
	tomlConfig = fmt.Sprintf(`
num-users = %d
`, numUsers)

	setViperConfigFromToml(t, tomlConfig)
	cfg = ReadConfig(logger)
	assert.Equal(t, []int{numUsers}, cfg.NumUsers)
}

func TestReadConfigUsesDefaults(t *testing.T) {
	resetViperConfig(t)

	logger := zaptest.NewLogger(t)
	cfg := ReadConfig(logger)

	expected := &Config{
		Username:              DefaultUsername,
		Password:              DefaultPassword,
		LogLevel:              DefaultLogLevel,
		Bucket:                DefaultBucket,
		Scope:                 DefaultScope,
		Collection:            DefaultCollection,
		RunTime:               DefaultRunTime,
		RampTime:              DefaultRampTime,
		OtlpEndpoint:          DefaultOtlpEndpoint,
		DialTimeout:           DefaultDialTimeout,
		ResponseHeaderTimeout: DefaultResponseHeaderTimeout,
		RequestTimeout:        DefaultRequestTimeout,
		IdleConnTimeout:       DefaultIdleConnTimeout,
		NumItems:              defaultNumItems,
		NumUsers:              []int{DefaultNumUsers},
	}

	assert.Equal(t, expected, cfg)
}

func TestClearDefaults(t *testing.T) {
	cfg := &Config{
		RunTime:      DefaultRunTime,
		RampTime:     DefaultRampTime,
		Username:     DefaultUsername,
		Password:     DefaultPassword,
		LogLevel:     DefaultLogLevel,
		Bucket:       DefaultBucket,
		Scope:        DefaultScope,
		Collection:   DefaultCollection,
		OtlpEndpoint: DefaultOtlpEndpoint,
		MarkovChain:  [][]float64{{1.0, 0.0}, {0.0, 1.0}},
	}

	defaultMarkov := [][]float64{{1.0, 0.0}, {0.0, 1.0}}

	clearDefaults(cfg, defaultMarkov)

	// Check that all default fields are cleared
	assert.Equal(t, "", cfg.RunTime)
	assert.Equal(t, "", cfg.RampTime)
	assert.Equal(t, "", cfg.Username)
	assert.Equal(t, "", cfg.Password)
	assert.Equal(t, "", cfg.LogLevel)
	assert.Equal(t, "", cfg.Bucket)
	assert.Equal(t, "", cfg.Scope)
	assert.Equal(t, "", cfg.Collection)
	assert.Equal(t, "", cfg.OtlpEndpoint)
	assert.Nil(t, cfg.MarkovChain)

	cfg = &Config{
		RunTime:      "10m",
		RampTime:     "1m",
		Username:     "custom-user",
		Password:     "custom-pass",
		LogLevel:     "debug",
		Bucket:       "custom-bucket",
		Scope:        "custom-scope",
		Collection:   "custom-collection",
		OtlpEndpoint: "custom-endpoint",
		MarkovChain:  [][]float64{{1.0, 0.0}, {0.1, 0.9}},
	}

	defaultMarkov = [][]float64{{1.0, 0.0}, {0.0, 1.0}}

	clearDefaults(cfg, defaultMarkov)

	// Check that non-default fields are preserved
	assert.Equal(t, "10m", cfg.RunTime)
	assert.Equal(t, "1m", cfg.RampTime)
	assert.Equal(t, "custom-user", cfg.Username)
	assert.Equal(t, "custom-pass", cfg.Password)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, "custom-bucket", cfg.Bucket)
	assert.Equal(t, "custom-scope", cfg.Scope)
	assert.Equal(t, "custom-collection", cfg.Collection)
	assert.Equal(t, "custom-endpoint", cfg.OtlpEndpoint)
	assert.Equal(t, [][]float64{{1.0, 0.0}, {0.1, 0.9}}, cfg.MarkovChain)
}

func TestWriteConfig(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "spectroperf-config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &Config{
		Connstr:     "couchbase://localhost",
		Workload:    "test-workload",
		NumItems:    100,
		NumUsers:    []int{1, 2},
		RunTime:     DefaultRunTime,
		RampTime:    DefaultRampTime,
		Username:    DefaultUsername,
		Password:    DefaultPassword,
		LogLevel:    DefaultLogLevel,
		Bucket:      DefaultBucket,
		Scope:       DefaultScope,
		Collection:  DefaultCollection,
		MarkovChain: [][]float64{{1.0, 0.0}, {0.0, 1.0}},
	}

	defaultMarkov := [][]float64{{1.0, 0.0}, {0.0, 1.0}}

	if err := WriteConfig(cfg, tempDir, defaultMarkov); err != nil {
		t.Fatalf("WriteConfig returned error: %v", err)
	}

	configPath := filepath.Join(tempDir, "config.toml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read written config: %v", err)
	}

	content := string(data)
	assert.Contains(t, content, "connstr", "expected non-default fields to be present in written config")
	assert.Contains(t, content, "workload", "expected non-default fields to be present in written config")
	assert.NotContains(t, content, "run-time", "expected default fields to be stripped from written config")
	assert.NotContains(t, content, "ramp-time", "expected default fields to be stripped from written config")
	assert.NotContains(t, content, "username", "expected default fields to be stripped from written config")
	assert.NotContains(t, content, "password", "expected default fields to be stripped from written config")

	var decoded Config
	if _, err := toml.Decode(content, &decoded); err != nil {
		t.Fatalf("failed to decode written toml: %v", err)
	}

	assert.Equal(t, "couchbase://localhost", decoded.Connstr)
	assert.Equal(t, "test-workload", decoded.Workload)
	assert.Equal(t, 100, decoded.NumItems)
}

func resetViperConfig(t *testing.T) {
	t.Helper()
	viper.Reset()
	flagSet := NewFlagSet()
	if err := BindFlagSet(flagSet); err != nil {
		t.Fatalf("failed to bind config flags: %v", err)
	}
}

func setViperConfigFromToml(t *testing.T, tomlConfig string) {
	t.Helper()
	resetViperConfig(t)
	viper.SetConfigType("toml")
	if err := viper.ReadConfig(strings.NewReader(tomlConfig)); err != nil {
		t.Fatalf("failed to read toml config: %v", err)
	}
}
