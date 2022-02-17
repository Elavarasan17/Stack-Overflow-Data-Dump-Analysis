# Stackoverflow Analysis!

Welcome to the StackOverflow Dump Analysis project. The following document serves as a guide to execute the workflow.


## Steps for GCP Setup

1. Go to https://cloud.google.com/ to start setting up your GCP platform. GCP offers $370 CAD in free credits to every new user. Please login in with your account and enable the free tier. [Required resources: Cloud storage (optional), Dataproc cluster, Big Query, Data studio].

2. Install [gcloud](https://cloud.google.com/sdk/docs/install) command line sdk in your machine. Enable the Dataproc API and create an instance. This step is done in the shell script (start-up-linux.<span>sh</span> for linux and start-up-mac.<span>sh</span> for mac) provided in the start-up directory.

A sample for mac:

```console
./start-up/start-up-mac.sh -n demo
```
The optional arguments (if you want any change) are:  
-r : Region you want the dataproc cluster in  
-w : number of workder nodes  
-n: Name of the cluster required  
-b : Bucket name if you want stored results in your own bucket. By default one of our public buckets is chosen. You will have to 
  
We reccomend that you create clusters and buckets in us-west1 due to cost advantages. The input bucket is also placed there.

create this [bucket](https://cloud.google.com/storage/docs/creating-buckets).  

Run your bash_profile file or start a new terminal session (to initialize the session) to continue the steps.

3. Create a [BigQuery dataset](https://cloud.google.com/bigquery/docs/datasets) to store the aggregated results.
 
## Executing the scripts

1. Export the following variables replacing the necessary values:
```
export BUCKET_NAME={YOUR-BUCKET-NAME}
```
```
export REGION={YOUR-CLUSTER -REGION}
```
```
export CLUSTER={YOUR-CLUSTER-NAME}
```
```
export DATASET={YOUR-BIGQUERY-DATASET-NAME}
```
```
export TEMP_BUCKET={YOUR-GCLOUD-TEMPORARY-BUCKET-NAME}
```
You can use the public bucket we have created to store your results.  
For example:  
export REGION="us-west1"  
export CLUSTER="stack-analysis-cluster"  
export BUCKET_NAME="data-processors-stackoverflow"  
A temporary bucket is usually always available in Google Storage once you a create a google acccount.  

2.  Run these commands:

### Preprocessing:

```
gcloud dataproc jobs submit pyspark spark/preprocessing/posts.py  --cluster=${CLUSTER} --properties=spark.jars.packages='com.databricks:spark-xml_2.12:0.14.0' --region=${REGION} -- -posts_src gs://data-processors-stackoverflow/inputs/Posts.xml -posts_dest gs://${BUCKET_NAME}/staging/posts/
```
```
gcloud dataproc jobs submit pyspark spark/preprocessing/users.py  --cluster=${CLUSTER} --properties=spark.jars.packages='com.databricks:spark-xml_2.12:0.14.0' --region=${REGION} -- -users_src gs://data-processors-stackoverflow/inputs/Users.xml -users_output gs://${BUCKET_NAME}/staging/users/
```
```
gcloud dataproc jobs submit pyspark spark/preprocessing/badges.py --cluster=${CLUSTER} --properties=spark.jars.packages='com.databricks:spark-xml_2.12:0.14.0' --region=${REGION} -- -xml_badges gs://data-processors-stackoverflow/inputs/Badges.xml -output_path gs://${BUCKET_NAME}/staging/badges/
```
```
gcloud dataproc jobs submit pyspark spark/preprocessing/comments.py --cluster=${CLUSTER} --properties=spark.jars.packages='com.databricks:spark-xml_2.12:0.14.0' --region=${REGION} -- -comments_src gs://data-processors-stackoverflow/inputs/Comments.xml -comments_dest gs://${BUCKET_NAME}/staging/comments/
```
```
gcloud dataproc jobs submit pyspark spark/preprocessing/postlinks.py --cluster=${CLUSTER} --properties=spark.jars.packages='com.databricks:spark-xml_2.12:0.14.0' --region=${REGION} -- -xml_postlinks gs://data-processors-stackoverflow/inputs/Postlinks.xml -output_path gs://${BUCKET_NAME}/staging/postlinks/
```
```
gcloud dataproc jobs submit pyspark spark/preprocessing/votes.py --cluster=${CLUSTER} --properties=spark.jars.packages='com.databricks:spark-xml_2.12:0.14.0' --region=${REGION} -- -votes_src gs://data-processors-stackoverflow/inputs/Votes.xml -vote_types_src gs://data-processors-stackoverflow/inputs/Votetypes.csv -votes_dest gs://${BUCKET_NAME}/staging/votes/
```

### Exploration:

```
gcloud dataproc jobs submit pyspark spark/user-answers.py  --cluster=${CLUSTER} --region=${REGION} --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar  -- -posts_src gs://${BUCKET_NAME}/staging/posts/ -users_src gs://${BUCKET_NAME}/staging/users/ -votes_src gs://${BUCKET_NAME}/staging/votes/postupdownvotes/ -tempbucket_src ${TEMP_BUCKET} -dataset_output ${DATASET}
```
```
gcloud dataproc jobs submit pyspark spark/explorer.py  --cluster=${CLUSTER} --region=${REGION} --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar -- -posts_src gs://${BUCKET_NAME}/staging/posts/ -users_src  gs://${BUCKET_NAME}/staging/users/ -tempbucket_src ${TEMP_BUCKET} -dataset_src ${DATASET}
```
```
gcloud dataproc jobs submit pyspark spark/badges-features.py  --cluster=${CLUSTER} --region=${REGION} --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar -- -input_users gs://${BUCKET_NAME}/staging/users/ -input_badges gs://${BUCKET_NAME}/staging/badges/ -output_dataset ${DATASET} -bucket_name ${TEMP_BUCKET}
```
```
gcloud dataproc jobs submit pyspark spark/badges-trends.py  --cluster=${CLUSTER} --region=${REGION} --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar -- -input_badges gs://${BUCKET_NAME}/staging/badges/ -output_dataset ${DATASET} -bucket_name ${TEMP_BUCKET}
```
```
gcloud dataproc jobs submit pyspark spark/duplicates.py --cluster=${CLUSTER} --region=${REGION} --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar -- -input_users gs://${BUCKET_NAME}/staging/users/ -input_posts gs://${BUCKET_NAME}/staging/posts/ -input_postlinks gs://${BUCKET_NAME}/staging/postlinks/ -output_dataset ${DATASET} -bucket_name ${TEMP_BUCKET}
```
```
gcloud dataproc jobs submit pyspark spark/badges-location.py  --cluster=${CLUSTER} --region=${REGION} --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar -- -input_users gs://${BUCKET_NAME}/staging/users/ -input_badges gs://${BUCKET_NAME}/staging/badges/ -output_dataset ${DATASET} -bucket_name ${TEMP_BUCKET}
```

3. Run this query in big query to generate additional aggregated data. Replace "eastern-memory-333522.stackoverflow.user_tag_expertise" with your own "projectid.bucket_name.user_tag_expertise".   
```
select * from `eastern-memory-333522.stackoverflow.user_tag_expertise` x inner join `eastern-memory-333522.stackoverflow.top10_user_details` y on x.id = y.id;
```


### ML Model building:

```
gcloud dataproc jobs submit pyspark spark/ml-model-builder.py  --cluster=${CLUSTER} --region=${REGION} -- -posts_src gs://${BUCKET_NAME}/staging/posts/ -users_src gs://${BUCKET_NAME}/staging/users/ -answered_dest gs://${BUCKET_NAME}/outputs/ml-data/answer-data/ -users_dest gs://${BUCKET_NAME}/outputs/ml-data/users/ -answer_predictor_dest gs://${BUCKET_NAME}/outputs/models/answer-predictor/ -user_predictor_dest gs://${BUCKET_NAME}/outputs/models/user-predictor/
```

### Web APP:

Go to the app directory and copy the data files from Google Storage:
```
cd app/
```
```
gsutil -m cp -r gs://${BUCKET_NAME}/outputs/ml-data/ . 
```
```
gsutil -m cp -r gs://${BUCKET_NAME}/outputs/models/ . 
```

#### Option 1

Install the requirements required:
```
pip install -r requirements.txt
```

Run the web aplication:
```
streamlit run app.py
```

#### Option 2

We have also setup a dockerfile if you don't have Java and Python and just need a container to run the web app. This requires [docker](https://docs.docker.com/get-docker/) to be installed.
```
docker build -t stackoverflow:1.0 .
```
```
docker run stackoverflow:1.0
```