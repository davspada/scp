#!/bin/sh
CLUSTER_NAME="scp-cluster"
REGION="europe-west1"
BUCKET_NAME="scp-bucket-$(whoami)"
JAR_NAME=$(gsutil ls gs://$BUCKET_NAME/*.jar | head -n 1)
DATASET="gs://$BUCKET_NAME/order_products.csv"
OUTPUT="gs://$BUCKET_NAME/output"

# Clean up output directory before running the job
echo "Deleting previous output (if any) at $OUTPUT ..."
gsutil -m rm -r "$OUTPUT" 2>/dev/null

# Get number of worker nodes (may be empty string for single-node)
NUM_WORKERS=$(gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION --format="value(config.workerConfig.numInstances)")
echo "NUM_WORKERS is: '$NUM_WORKERS'"

EXECUTOR_CORES=4
EXECUTOR_MEMORY=4g

if [ -z "$NUM_WORKERS" ] || [ "$NUM_WORKERS" = "0" ]; then
  echo "Submitting Spark job in single-node mode (master only)..."
  gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --class=Main \
    --jars=$JAR_NAME \
    --properties=spark.executor.instances=1,spark.executor.cores=2,spark.executor.memory=3g \
    -- $DATASET $OUTPUT
    # ,spark.driver.memory=3g
else
  echo "Submitting Spark job for multi-worker cluster ($NUM_WORKERS workers)..."
  gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --class=Main \
    --jars=$JAR_NAME \
    --properties=spark.executor.instances=$NUM_WORKERS,spark.executor.cores=$EXECUTOR_CORES,spark.executor.memory=$EXECUTOR_MEMORY \
    -- $DATASET $OUTPUT
fi