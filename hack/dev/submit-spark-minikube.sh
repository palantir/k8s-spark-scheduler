#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail


IMAGE=$1
SPARK_VERSION=$2
# spark-submit must point to a spark distribution which has k8s support
spark-submit \
  --master k8s://https://192.168.99.100:8443 \
  --deploy-mode cluster \
  --name spark-pi \
  --class "org.apache.spark.examples.SparkPi" \
  --conf "spark.kubernetes.authenticate.driver.serviceAccountName=spark" \
  --conf "spark.executor.instances=2" \
  --conf "spark.driver.cores=0.1" \
  --conf "spark.kubernetes.container.image=$IMAGE" \
  local:///opt/spark/examples/jars/spark-examples_2.11-$SPARK_VERSION.jar
