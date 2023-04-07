package types

import "github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"

type SparkApplicationResources struct {
	DriverResources   *resources.Resources
	ExecutorResources *resources.Resources
	MinExecutorCount  int
	MaxExecutorCount  int
}
