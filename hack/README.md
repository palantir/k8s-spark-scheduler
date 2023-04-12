If you're trying to run a local version of `k8s-spark-scheduler` in
[`minikube`](https://github.com/kubernetes/minikube), here are a few pointers that might be helpful (ideally they'd be
encoded in the various scripts, but for now some notes is better than nothing):

* Start `minikube` with `minikube start --kubernetes-version 1.20.0` as the config examples under `examples/` do not
  currently work with k8s 1.25.0 (to be clear `k8s-spark-scheduler` is known to work up to at least k8s 1.24.0).
* Run `eval $(minikube docker-env)` to ensure your locally built docker images get pushed to correct docker daemon.
* Run
  [`./hack/dev/run-in-minikube.sh`](https://github.com/palantir/k8s-spark-scheduler/blob/master/hack/dev/run-in-minikube.sh)
  (this will build the repo/docker images and deploy the relevant k8s resources).
* Given that
  [`examples/extender.yml`](https://github.com/palantir/k8s-spark-scheduler/blob/master/examples/extender.yml) uses
  `:latest` version tags, you might also need to run something like `minikube image tag 99f76eda67953 docker.io/palantirtechnologies/spark-scheduler:latest`
  to properly tag your images.
  If you do so, you'll want to destroy your current `k8s-spark-scheduler` pods, e.g. `kubectl delete po -l app=spark-scheduler`,
  those will then be replaced automatically.
* Submit a fake spark application (this doesn't actually run Spark, only `nginx` containers) to k8s with `./examples/submit-test-spark-app.sh foobar 1 .1 100M 0 .1 100M 0`.
