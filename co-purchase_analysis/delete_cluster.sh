#!/bin/sh

CLUSTER_NAME="scp-cluster"
REGION="europe-west1"

gcloud dataproc clusters delete $CLUSTER_NAME --region $REGION --quiet