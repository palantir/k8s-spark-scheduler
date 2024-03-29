#!/usr/bin/env bash

# docker build, push and start the spark scheduler extender on a running minikube cluster
# minikube has to be running

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname ${BASH_SOURCE})/../..
GENERATED_KEYS_DIR=${SCRIPT_ROOT}/out/generated_keys/


$SCRIPT_ROOT/hack/dev/generate-certs.sh

#eval $(minikube docker-env)
$SCRIPT_ROOT/godelw docker build --verbose

kubectl apply -f $SCRIPT_ROOT/examples/namespace.yml

kubectl create configmap scheduler-secrets --namespace=spark --from-file="${GENERATED_KEYS_DIR}"
kubectl create configmap spark-scheduler-conversion-webhook-secrets --namespace=spark --from-file="${GENERATED_KEYS_DIR}"

kubectl apply -f $SCRIPT_ROOT/examples/extender.yml
