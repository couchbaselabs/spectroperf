package configuration

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"go.uber.org/zap"
)

const epsilon = 1e-9

// CreateMarkovChain determines which markov chain to use based on the config.
func CreateMarkovChain(logger *zap.Logger, config *Config, operations []string, probabilities [][]float64) ([][]float64, error) {
	var markovChain [][]float64
	if len(config.MarkovChain) != 0 {
		if err := validateMarkovChain(logger, len(operations), config.MarkovChain); err != nil {
			return nil, fmt.Errorf("invalid markov chain: %w", err)
		}
		markovChain = config.MarkovChain
	}

	if config.OnlyOperation != "" {
		if len(config.MarkovChain) != 0 {
			return nil, errors.New("cannot specify only-operation and a markov chain")
		}

		var err error
		markovChain, err = buildMarkovChain(logger, config.OnlyOperation, operations)
		if err != nil {
			return nil, fmt.Errorf("building markov chain: %w", err)
		}
	}

	if len(markovChain) == 0 {
		logger.Info("neither markov chain nor only operation specified, using built in workload probabilities")
		markovChain = probabilities
	}

	return markovChain, nil
}

// validateMarkov chain checks that the markov chain from the config file
// valid by making sure that:
// - all rows sum to 1
// - is square
// - has dimensions equal to number of workload operations
func validateMarkovChain(logger *zap.Logger, workloadOperations int, mChain [][]float64) error {
	logger.Info("validating Markov chain from config file")

	dimensionError := fmt.Errorf("Markov chain must be square array with dimensions equal to number of workload operations")

	if len(mChain) != workloadOperations {
		return dimensionError
	}

	for _, row := range mChain {
		if len(row) != workloadOperations {
			return dimensionError
		}

		var total float64
		for _, probability := range row {
			if probability < 0 || probability > 1 {
				return fmt.Errorf("Markov Chain probabilities must be between 0 and 1")
			}

			total += probability
		}

		if math.Abs(total-1) > epsilon {
			return fmt.Errorf("Markov Chain row does not sum to 1: %v", row)
		}
	}

	return nil
}

// buildMarkovChain builds a markov chain that will only perform the named
// operation from the chosen workload
func buildMarkovChain(logger *zap.Logger, operation string, operations []string) ([][]float64, error) {
	logger.Info("building markov chain to perform one operation", zap.String("operation", operation))

	opIndex := slices.Index(operations, operation)
	if opIndex == -1 {
		return nil, fmt.Errorf("chosen only-operation '%s' is not supported by workload", operation)
	}

	markovChain := make([][]float64, len(operations))
	row := make([]float64, len(operations))
	row[opIndex] = 1.0
	for i := 0; i < len(operations); i++ {
		markovChain[i] = row
	}

	return markovChain, nil
}
