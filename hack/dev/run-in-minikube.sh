#!/usr/bin/env bash

# docker build, push and start the spark scheduler extender on a running minikube cluster
# minikube has to be running

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname ${BASH_SOURCE})/../..

$SCRIPT_ROOT/hack/dev/generate-certs.sh

#eval $(minikube docker-env)
$SCRIPT_ROOT/godelw docker build --verbose

kubectl apply -f $SCRIPT_ROOT/examples/namespace.yml

kubectl create configmap scheduler-secrets --namespace=spark --from-file="${SCRIPT_ROOT}/out/tmp/generated_keys/"
kubectl create configmap resource-reservation-conversion-webhook-secrets --namespace=spark --from-file="${SCRIPT_ROOT}/out/tmp/generated_keys/"

kubectl apply -f $SCRIPT_ROOT/examples/extender.yml
