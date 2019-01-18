#!/usr/bin/env bash

# live-reloads a running spark-scheduler extender pod by restarting the pod

set -o errexit
set -o nounset
set -o pipefail

function getpod {
  kubectl get pod --all-namespaces | grep spark-scheduler | grep -v Terminating | awk '{print $2}'
}

OLD_PODNAME="$(getpod)"
echo $OLD_PODNAME
kubectl delete pod -n kube-system "$OLD_PODNAME"

while true; do
  PODNAME="$(getpod)"
  if [ "$PODNAME" = "" ]; then
    echo "pod has not started yet"
    sleep 1
  else
    sleep 3
    echo "tailing logs"
    kubectl logs -f -c spark-scheduler-extender "$PODNAME" -n kube-system
    exit
  fi
done
