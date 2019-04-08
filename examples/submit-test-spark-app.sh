#!/usr/bin/env bash

# submit-test-spark-app <app-id> <executor-count> <driver-cpu> <driver-mem> <executor-cpu> <executor-mem>
set -o errexit
set -o nounset
set -o pipefail

APP_ID=$1
EXECUTOR_COUNT=$2
DRIVER_CPU=$3
DRIVER_MEM=$4
EXECUTOR_CPU=$5
EXECUTOR_MEM=$6

# create driver
kubectl create -f <(cat << EOF
apiVersion: v1
kind: Pod
metadata:
  name: test-driver-$APP_ID
  labels:
    spark-role: "driver"
    spark-app-id: "$APP_ID"
  annotations:
    spark-driver-cpu: "$DRIVER_CPU"
    spark-driver-mem: "$DRIVER_MEM"
    spark-executor-cpu: "$EXECUTOR_CPU"
    spark-executor-mem: "$EXECUTOR_MEM"
    spark-executor-count: "$EXECUTOR_COUNT"
spec:
  schedulerName: spark-scheduler
  containers:
  - name: nginx
    image: nginx
    ports:
    - containerPort: 80
    resources:
      requests:
        cpu: "$DRIVER_CPU"
        memory: "$DRIVER_MEM"
EOF)

# wait for driver to be running
until grep 'Running' <(kubectl get pod test-driver-$APP_ID -o=jsonpath='{.status.phase}'); do
  sleep 1
done
DRIVER_UID=$(kubectl get pod test-driver-$APP_ID -o=jsonpath='{.metadata.uid}')

# create executors
for i in $(seq "$EXECUTOR_COUNT"); do
  kubectl create -f <(cat << EOF
apiVersion: v1
kind: Pod
metadata:
  name: test-executor-$APP_ID-$i
  labels:
    spark-role: "executor"
    spark-app-id: "$APP_ID"
  ownerReferences:
  - apiVersion: core/v1
    kind: Pod
    name: "test-driver-$APP_ID"
    uid: $DRIVER_UID
spec:
  schedulerName: spark-scheduler
  containers:
  - name: nginx
    image: nginx
    ports:
    - containerPort: 80
    resources:
      requests:
        cpu: "$EXECUTOR_CPU"
        memory: "$EXECUTOR_MEM"
EOF)
done

