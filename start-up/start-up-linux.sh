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

sudo apt-get install apt-transport-https ca-certificates gnupg

# Add the gcloud CLI distribution URI as a package source.
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud public key.
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Update and install the gcloud CLI:
sudo apt-get update && sudo apt-get install google-cloud-sdk
gcloud init

# Enable the services
gcloud services enable dataproc.googleapis.com
gcloud services enable bigquery.googleapis.com

#Creating the cluster
gcloud dataproc clusters create ${name} --enable-component-gateway --region ${region} --master-machine-type n1-standard-4 \
--master-boot-disk-size 500 --num-workers ${num_workers} --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 2.0-debian10 \
--optional-components JUPYTER

#Exporting the variables
export BUCKET_NAME=${bucket_name}
export REGION=${region} 
export CLUSTER=${name}