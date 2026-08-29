package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	semantics "simq.dev/sqssemantics"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	backendName := flag.String("backend", "simq", "backend: simq or aws")
	endpointList := flag.String("simq-endpoints", "http://127.0.0.1:19324,http://127.0.0.1:19325,http://127.0.0.1:19326", "comma-separated SimQ API endpoints")
	region := flag.String("aws-region", "", "AWS region; required for the aws backend")
	allowAWSMutation := flag.Bool("allow-aws-mutation", false, "allow creation and deletion of disposable AWS SQS queues")
	prefix := flag.String("queue-prefix", "simq-poc", "disposable queue name prefix")
	timeout := flag.Duration("timeout", 12*time.Minute, "whole-suite timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var backend semantics.Backend
	switch *backendName {
	case "simq":
		endpoints := splitNonEmpty(*endpointList)
		configured, err := semantics.NewSimQBackend(semantics.SimQOptions{
			Endpoints: endpoints,
			Token:     os.Getenv("SIMQ_POC_TOKEN"),
		})
		if err != nil {
			return fmt.Errorf("configure SimQ backend: %w", err)
		}
		backend = configured
	case "aws":
		configured, err := semantics.NewAWSBackend(ctx, semantics.AWSOptions{Region: *region, AllowMutations: *allowAWSMutation})
		if err != nil {
			return fmt.Errorf("configure AWS backend: %w", err)
		}
		backend = configured
	default:
		return errors.New("backend must be simq or aws")
	}

	runner, err := semantics.NewRunner(backend, *prefix)
	if err != nil {
		return fmt.Errorf("configure scenario runner: %w", err)
	}
	report, runErr := runner.Run(ctx)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return errors.New("encode redacted result report failed")
	}
	if runErr != nil {
		return errors.New("one or more SQS semantics scenarios failed; inspect the redacted JSON report")
	}
	return nil
}

func splitNonEmpty(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}
