package main

import (
	"github.com/palantir/k8s-spark-scheduler/resource-reservation-conversion-webhook/cmd"
	"github.com/palantir/pkg/signals"
	"os"
)

func main() {
	signals.RegisterStackTraceWriter(os.Stderr, nil)
	os.Exit(cmd.Execute())
}
