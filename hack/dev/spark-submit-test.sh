#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

EXECUTOR_COUNT=$1
DRIVER_CPU=$2
DRIVER_MEM=$3 # in mb
EXECUTOR_CPU=$4
EXECUTOR_MEM=$5 # in mb

TEMPLATE=$(cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  labels:
    spark-app-id: $RANDOM
spec:
  schedulerName: spark-scheduler
EOF
)


$SPARK_HOME/bin/spark-submit \
  --master k8s://https://localhost:6443 \
  --deploy-mode cluster \
  --name spark-test \
  --class "org.apache.spark.examples.DriverAndExecutorsSubmissionTest" \
  --conf "spark.kubernetes.container.image=spark:test" \
  --conf "spark.kubernetes.driver.podTemplateFile="<(echo "$TEMPLATE") \
  --conf "spark.kubernetes.executor.podTemplateFile="<(echo "$TEMPLATE") \
  --conf "spark.executor.instances=$EXECUTOR_COUNT" \
  --conf "spark.driver.cores=$DRIVER_CPU" \
  --conf "spark.driver.memory=$DRIVER_MEM"m \
  --conf "spark.executor.cores=$EXECUTOR_CPU" \
  --conf "spark.executor.memory=$EXECUTOR_MEM"m \
  --conf "spark.kubernetes.driver.annotation.spark-executor-count=$EXECUTOR_COUNT" \
  --conf "spark.kubernetes.driver.annotation.spark-driver-cpu=$DRIVER_CPU" \
  --conf "spark.kubernetes.driver.annotation.spark-driver-mem=$DRIVER_MEM"M \
  --conf "spark.kubernetes.driver.annotation.spark-executor-cpu=$EXECUTOR_CPU" \
  --conf "spark.kubernetes.driver.annotation.spark-executor-mem=$EXECUTOR_MEM"M \
  local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0-SNAPSHOT.jar \
  60
