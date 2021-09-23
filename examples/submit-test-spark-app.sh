#!/usr/bin/env bash

# submit-test-spark-app <app-id> <executor-count> <driver-cpu> <driver-mem> <executor-cpu> <executor-mem>
set -o errexit
set -o nounset
set -o pipefail

APP_ID=$1
EXECUTOR_COUNT=$2
DRIVER_CPU=$3
DRIVER_MEM=$4
DRIVER_NVIDIA_GPUS=$5
EXECUTOR_CPU=$6
EXECUTOR_MEM=$7
EXECUTOR_NVIDIA_GPUS=$8

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
    spark-driver-nvidia.com/gpu: "$DRIVER_NVIDIA_GPUS"
    spark-executor-cpu: "$EXECUTOR_CPU"
    spark-executor-mem: "$EXECUTOR_MEM"
    spark-executor-nvidia.com/gpu: "$EXECUTOR_NVIDIA_GPUS"
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
        nvidia.com/gpu: "$DRIVER_NVIDIA_GPUS"
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
        nvidia.com/gpu: "$EXECUTOR_NVIDIA_GPUS"
EOF)
done

