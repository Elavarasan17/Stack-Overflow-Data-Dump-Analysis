#!/bin/bash

region='us-west1'
num_workers=4
name="stack-analysis-cluster"
bucket_name="stackoverflow-data"

print_usage() {
    "Using defaults"
}

# Get the command line arguments
while getopts 'n:w:r:b:' flag; do
  case "${flag}" in
    r) region="${OPTARG}" ;;
    w) num_workers="${OPTARG}" ;;
    n) name="${OPTARG}" ;;
    b) bucket_name="${OPTARG}" ;;
    *) print_usage
       exit 1 ;;
  esac
done

# Remove the existing cloud sdk
rm -f google-cloud-sdk-365.0.1-darwin-x86_64.tar.gz 

# Download the package
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-365.0.1-darwin-x86_64.tar.gz
tar -zxf google-cloud-sdk-365.0.1-darwin-x86_64.tar.gz
rm google-cloud-sdk-365.0.1-darwin-x86_64.tar.gz

# Install gcloud cli tools
./google-cloud-sdk/install.sh
./google-cloud-sdk/bin/gcloud init

# Enable the cloud services
./google-cloud-sdk/bin/gcloud services enable dataproc.googleapis.com
./google-cloud-sdk/bin/gcloud services enable bigquery.googleapis.com

# Creating the cluster
./google-cloud-sdk/bin/gcloud dataproc clusters create ${name} --enable-component-gateway --region ${region} --master-machine-type n1-standard-4 \
--master-boot-disk-size 500 --num-workers ${num_workers} --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 2.0-debian10 \
--optional-components JUPYTER

# Exporting the variables
export BUCKET_NAME=${bucket_name}
export REGION=${region} 
export CLUSTER=${name}