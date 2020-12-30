
# Reading unstructured data using pyspark and submitting the job

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

Downloaded the dataset from the following:
17 Category flower dataset (1362 images)
https://www.robots.ox.ac.uk/~vgg/data/flowers/17/index.html

Wrote the following script to read file from google cloud storage 

```
import pyspark
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Flower Images').getOrCreate()

path = "gs://dataproc-nyc-taxi-2020/images/"
image_df = spark.read.format("image").load(path)

#print Flower image data schema
image_df.printSchema()

#display count of images
image_df.count()

#show first 5 image dataset
image_df.show(5)

```
Ran the job using the following command:

`gcloud dataproc jobs submit pyspark image.py --cluster=${CLUSTER} --region=${REGION}
`

Output is as follows:

```
Job Id:  3e84638d4e554555952276fc18f1b59e

20/12/30 10:34:02 INFO org.spark_project.jetty.util.log: Logging initialized @2933ms
20/12/30 10:34:02 INFO org.spark_project.jetty.server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
20/12/30 10:34:02 INFO org.spark_project.jetty.server.Server: Started @3023ms
20/12/30 10:34:02 INFO org.spark_project.jetty.server.AbstractConnector: Started ServerConnector@73551a2{HTTP/1.1,[http/1.1]}{0.0.0.0:39017}
20/12/30 10:34:03 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-58d6-m/10.128.0.6:8032
20/12/30 10:34:04 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at cluster-58d6-m/10.128.0.6:10200
20/12/30 10:34:04 INFO org.apache.hadoop.conf.Configuration: resource-types.xml not found
20/12/30 10:34:04 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Unable to find 'resource-types.xml'.
20/12/30 10:34:04 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = memory-mb, units = Mi, type = COUNTABLE
20/12/30 10:34:04 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = vcores, units = , type = COUNTABLE
20/12/30 10:34:05 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1609311745804_0003
root
 |-- image: struct (nullable = true)
 |    |-- origin: string (nullable = true)
 |    |-- height: integer (nullable = true)
 |    |-- width: integer (nullable = true)
 |    |-- nChannels: integer (nullable = true)
 |    |-- mode: integer (nullable = true)
 |    |-- data: binary (nullable = true)

+--------------------+
|               image|
+--------------------+
|[gs://dataproc-ny...|
|[gs://dataproc-ny...|
|[gs://dataproc-ny...|
|[gs://dataproc-ny...|
|[gs://dataproc-ny...|
+--------------------+
only showing top 5 rows

20/12/30 10:34:28 INFO org.spark_project.jetty.server.AbstractConnector: Stopped Spark@73551a2{HTTP/1.1,[http/1.1]}{0.0.0.0:0}
Job output is complete
```
