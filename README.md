# Kubernetes Spark Scheduler Extender

[![CircleCI](https://circleci.com/gh/palantir/k8s-spark-scheduler.svg?style=svg)](https://circleci.com/gh/palantir/k8s-spark-scheduler)

`k8s-spark-scheduler-extender` is a [Kubernetes Scheduler Extender](https://github.com/kubernetes/community/blob/master/contributors/design-proposals/scheduling/scheduler_extender.md) that is designed to provide gang scheduling capabilities for running Apache Spark on Kubernetes.

Running Spark applications at scale on Kubernetes with the default `kube-scheduler` is prone to resource starvation and oversubscription. Naively scheduling driver pods can occupy space that should be reserved for their executors. Using `k8s-spark-scheduler-extender` guarantees that a driver will only be scheduled if there is space in the cluster for all of its executors. It can also guarantee scheduling order for drivers, with respect to their creation timestamp.

Requirements:
- Kubernetes: 1.11.0
- Spark: Any snapshot build that includes commit [f6cc354d83](https://github.com/apache/spark/commit/f6cc354d83). This is expected to be in Spark 3.x

Spark scheduler extender is a [Witchcraft](https://github.com/palantir/witchcraft-go-server) server, and uses [Godel](https://github.com/palantir/godel) for testing and building. It is meant to be deployed with a new `kube-scheduler` instance, running alongside the default scheduler. This way, non-spark pods can continue to be scheduled by the default scheduler, and opt-in pods are scheduled using the spark-sdcheduler.

## Usage

To set up the scheduler extender as a new scheduler named `spark-scheduler`, run:
```sh
kubectl apply -f examples/extender.yml
```
This will create a new service account, a cluster binding for permissions, a config map and a deployment, all under namespace `kube-system`. It is worth noting that this example sets up the new scheduler with a super user.

Refer to [Spark's website](https://spark.apache.org/docs/2.3.0/running-on-kubernetes.html) for documentation on running Spark with Kubernetes. To schedule a spark application using spark-scheduler, you must apply the following metadata to driver and executor pods.
### driver:
```yml
apiVersion: v1
kind: Pod
metadata:
  labels:
    spark-app-id: my-custom-id
  annotations:
    spark-driver-cpus: 1
    spark-driver-mem: 1g
    spark-executor-cpu: 2
    spark-executor-mem: 4g
    spark-executor-count: 8
spec:
  schedulerName: spark-scheduler
```
### executor:
```yml
apiVersion: v1
kind: Pod
metadata:
  labels:
    spark-app-id: my-custom-id
spec:
  schedulerName: spark-scheduler
```

As of [f6cc354d83](https://github.com/apache/spark/commit/f6cc354d83), spark supports specifying pod templates for driver and executors. Although spark configuration can also be used to apply label and annotations, the pod template feature in spark is the only way of setting schedulerName. To apply the above overrides, you should save them as files and set these configuration overrides:
```
"spark.kubernetes.driver.podTemplateFile": "/path/to/driver.template",
"spark.kubernetes.executor.podTemplateFile": "/path/to/executor.template"
```

## Configuration

`k8s-spark-scheduler-extender` is a witchcraft service, and supports configuration options detailed in the [github documentation](https://github.com/palantir/witchcraft-go-server#configuration). Additional configuration options are:
 - fifo: a boolean flag to turn on FIFO processing of spark drivers. With this turned on, younger spark drivers will be blocked from scheduling until the cluster has space for the oldest spark driver. Executor scheduling is unaffected from this.
 - kube-config: path to a [kube-config file](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/)
 - binpack: the algorithm to binpack pods in a spark application over the free space in the cluster. Currently available options are `distribute-evenly` and `tightly-pack`, the former being the default. They differ on how they distribute the executors, `distribute-evenly` round-robin's available nodes, whereas `tightly-pack` fills one node before moving to the next.
 - qps and burst: These are parameters for rate limiting kubernetes clients, used directly in client construction.

## Development

Use `./godelw docker build` to build an image using the [Dockerfile template](docker/Dockerfile). Built image will use the [default configuration](docker/var/conf/install.yml). Deployment created by `kubectl apply -f examples/extender.yml` can be used to iterate locally.

Use `./examples/submit-test-spark-app.sh <id> <executor-count> <driver-cpu> <driver-mem> <executor-cpu> <executor-mem>` to mock a spark application launch.

Use `./godelw verify` to run tests and style checks

# Contributing

The team welcomes contributions!  To make changes:

- Fork the repo and make a branch
- Write your code (ideally with tests) and make sure the CircleCI build passes
- Open a PR (optionally linking to a github issue)

# License
This project is made available under the [Apache 2.0 License](/LICENSE).
