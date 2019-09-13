#!/usr/bin/env bash

export BUCKET=BUCKET_NAME_HERE
export PROJECT=PROJECT_ID_HERE

go run main.go \
  --job_name demo-job \
  --runner dataflow \
  --max_num_workers 20 \
  --file gs://${BUCKET?}/ratings.csv \
  --output gs://${BUCKET?}/reporting.txt \
  --project ${PROJECT?} \
  --temp_location gs://${BUCKET?}/tmp/ \
  --staging_location gs://${BUCKET?}/binaries/ \
  --worker_harness_container_image=gcr.io/${PROJECT?}/beam/go:latest
