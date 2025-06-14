#!/bin/sh

# Usage: ./create_cluster.sh <num_nodes>
NUM_NODES=$1
if [ -z "$NUM_NODES" ]; then
  echo "Usage: $0 <num_nodes>"
  exit 1
fi

CLUSTER_NAME="scp-cluster"
REGION="europe-west1"
ZONE="europe-west1-b"

if [ "$NUM_NODES" -eq 1 ]; then
  # Single-node cluster (master only)
  gcloud dataproc clusters create $CLUSTER_NAME \
    --region $REGION \
    --zone $ZONE \
    --master-machine-type n2-standard-4 \
    --single-node \
    --master-boot-disk-size 500
else
  # 1 master + NUM_NODES workers
gcloud dataproc clusters create $CLUSTER_NAME \
  --region $REGION \
  --zone $ZONE \
  --master-machine-type n2-standard-2 \
  --worker-machine-type n2-standard-4 \
  --num-workers $NUM_NODES \
  --max-idle=30m \
  --master-boot-disk-size 240 \
  --worker-boot-disk-size 240
fi

echo "Cluster $CLUSTER_NAME created with $NUM_NODES worker node(s) and 1 master"