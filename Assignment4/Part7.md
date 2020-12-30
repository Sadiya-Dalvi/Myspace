
# Converting CSV file to parquet using Pyspark

Created a dataproc cluster using the following command in cloud shell:

```
gcloud beta dataproc clusters create ${CLUSTER} \
  --region=${REGION} \
  --image-version=1.5 \
  --master-machine-type=n1-standard-4 \
  --master-boot-disk-size=50GB \
  --worker-machine-type=n1-standard-2 \
  --worker-boot-disk-size=50GB \
  --bucket=${BUCKET_NAME} \
  --optional-components=ANACONDA,JUPYTER \
  --enable-component-gateway
 ```
<kbd>
<img src="https://github.com/Sadiya-Dalvi/SDProfile/blob/main/Images/dataproc_part2.png" alt="dataproc_cluster" width="700" height="300" >
</kbd>


Wrote the following script to perform required task:

```
import pyspark
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('csvtoprq').getOrCreate()

#read csv with options
nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020/yellow_tripdata_2019-01.csv", header=True, inferSchema=True)
print("There are {} rows in the Dataframe from csv file.".format(nyctaxi_df.count()))
nyctaxi_df.printSchema()
  #convert to parquet

nyctaxi_df.write.parquet("gs://dataproc-nyc-taxi-2020/yellow_tripdata_2019-01-001.parquet")

#read parquet file from cloud storage
nycnew_df = spark.read.parquet("gs://dataproc-nyc-taxi-2020/yellow_tripdata_2019-01-001.parquet")

nycnew_df.printSchema()

print("There are {} rows in the Dataframe read from Parquet file.".format(nycnew_df.count()))

```
 
Ran the job using the following command:

`gcloud dataproc jobs submit pyspark csv_parquet.py --cluster=${CLUSTER} --region=${REGION}
`

Output is as follows:

```
Job Id: 692c9bf629dc49c8a565c6dbd6c18126

20/12/30 14:27:00 INFO org.spark_project.jetty.util.log: Logging initialized @3275ms
20/12/30 14:27:00 INFO org.spark_project.jetty.server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
20/12/30 14:27:00 INFO org.spark_project.jetty.server.Server: Started @3381ms
20/12/30 14:27:00 INFO org.spark_project.jetty.server.AbstractConnector: Started ServerConnector@73551a2{HTTP/1.1,[http/1.1]}{0.0.0.0:41989}
20/12/30 14:27:01 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-58d6-m/10.128.0.8:8032
20/12/30 14:27:01 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at cluster-58d6-m/10.128.0.8:10200
20/12/30 14:27:01 INFO org.apache.hadoop.conf.Configuration: resource-types.xml not found
20/12/30 14:27:01 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Unable to find 'resource-types.xml'.
20/12/30 14:27:01 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = memory-mb, units = Mi, type = COUNTABLE
20/12/30 14:27:01 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = vcores, units = , type = COUNTABLE
20/12/30 14:27:03 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1609329962763_0010
There are 7667792 rows in the Dataframe from csv file.
root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)

root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)

There are 7667792 rows in the Dataframe read from Parquet file.
20/12/30 14:29:47 INFO org.spark_project.jetty.server.AbstractConnector: Stopped Spark@73551a2{HTTP/1.1,[http/1.1]}{0.0.0.0:0}
Job output is complete

```

The parquet files were uploaded in the cloud storage as shown below:

<kbd>
<img src="https://github.com/Sadiya-Dalvi/SDProfile/blob/main/Images/csv_pq.jpeg" alt="dataproc_cluster" width="700" height="300" >
</kbd>
