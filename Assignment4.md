# Create a Datalake using Bigquery and Dataproc

Reading and writing data from BigQuery

Create a cluster with the bigquery connector using the following command in google cloud shell

```
gcloud beta dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --zone=${ZONE} \
  --image-version=1.5 \
  --master-machine-type=n1-standard-1 \
  --master-boot-disk-size=50GB \
  --worker-machine-type=n1-standard-1 \
  --worker-boot-disk-size=50GB \
  --bucket=${BUCKET_NAME} \
  --optional-components=ANACONDA,JUPYTER \
  --enable-component-gateway \
  --metadata 'PIP_PACKAGES=google-cloud-bigquery google-cloud-storage' \
  --metadata gcs-connector-version=2.1.1 \
  --metadata bigquery-connector-version=1.1.1 \
  --metadata spark-bigquery-connector-version=0.13.1-beta \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh,gs://nyc-taxi-data-sadiya/tez.sh  \
  --properties "spark:spark.jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar" 
  
  ```
