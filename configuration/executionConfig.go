package configuration

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// ExecutionConfig holds the parsed and validated configuration needed to execute a workload.
type ExecutionConfig struct {
	RunTime    time.Duration
	RampTime   time.Duration
	Sleep      time.Duration
	NumUsers   int
	NumItems   int
	Scope      string
	Collection string
}

// CreateExecutionConfig validates the raw config and produces an ExecutionConfig.
func CreateExecutionConfig(logger *zap.Logger, config *Config) (*ExecutionConfig, error) {
	if config.Connstr == "" {
		return nil, errors.New("no connection string provided")
	}

	if config.NumUsers <= 0 {
		return nil, errors.New("number of users must be greater than 0")
	}

	if config.NumItems <= 0 {
		return nil, errors.New("number of items must be greater than 0")
	}

	var execSleep time.Duration
	if config.Sleep == "" {
		logger.Info("no sleep set, random sleep duration will be used")
	} else {
		var err error
		execSleep, err = time.ParseDuration(config.Sleep)
		if err != nil {
			return nil, fmt.Errorf("parsing sleep duration from config: %w", err)
		}

		if execSleep < time.Duration(time.Millisecond*100) {
			return nil, errors.New("sleep cannot be less than 100ms, to increase throughput increase number of users")
		}
	}

	execRunTime, err := time.ParseDuration(config.RunTime)
	if err != nil {
		return nil, fmt.Errorf("parsing run time duration from config: %w", err)
	}

	if execRunTime <= 0 {
		return nil, errors.New("run time must be greater than 0")
	}

	execRampTime, err := time.ParseDuration(config.RampTime)
	if err != nil {
		return nil, fmt.Errorf("parsing ramp time duration from config: %w", err)
	}

	if execRampTime > execRunTime/2 {
		return nil, errors.New("ramp time cannot be greater than half of the total runtime")
	}

	return &ExecutionConfig{
		RunTime:    execRunTime,
		RampTime:   execRampTime,
		Sleep:      execSleep,
		NumUsers:   config.NumUsers,
		NumItems:   config.NumItems,
		Scope:      config.Scope,
		Collection: config.Collection,
	}, nil
}
