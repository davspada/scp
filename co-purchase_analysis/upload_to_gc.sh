#!/bin/sh

# Configuration
BUCKET_NAME="scp-bucket-$(whoami)"
REGION="europe-west1"
DATASET_PATH="./src/order_products.csv"
JAR_PATH=$(find target/scala-2.12 -name "*.jar" | head -n 1)

# Create bucket if it doesn't exist
if ! gsutil ls -b gs://$BUCKET_NAME >/dev/null 2>&1; then
  echo "Creating bucket: $BUCKET_NAME in $REGION"
  gsutil mb -l $REGION gs://$BUCKET_NAME/
fi

# Upload dataset and jar
echo "Uploading dataset..."
gsutil cp "$DATASET_PATH" gs://$BUCKET_NAME/
echo "Uploading jar: $JAR_PATH"
gsutil cp "$JAR_PATH" gs://$BUCKET_NAME/

echo "Done. Files in gs://$BUCKET_NAME/"