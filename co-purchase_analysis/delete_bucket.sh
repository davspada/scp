#!/bin/sh

BUCKET_NAME="scp-bucket-$(whoami)"

gsutil -m rm -r gs://$BUCKET_NAME/