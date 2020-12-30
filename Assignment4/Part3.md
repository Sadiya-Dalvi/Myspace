
# Reading structured data (Parquet) using pyspark and submitting the job

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

Wrote the following script to read parquet file from google cloud storage 

```
import pyspark
import sys


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('user-parquet').getOrCreate()

df = spark.read.parquet("gs://dataproc-nyc-taxi-2020/userdata1.parquet")

#print (df.dtypes)
#print (df.describe)

df.printSchema()

df.groupby('country').agg({'salary': 'mean'}).show()

df.groupby('country').count().show()

df.createOrReplaceTempView('user2')

df2 = spark.sql("SELECT country, last_name, first_name, salary \
                   FROM user2 \
                   WHERE salary > 1000")

df2.show()

```
Ran the job using the following command:

`gcloud dataproc jobs submit pyspark upload_parquet.py     --cluster=${CLUSTER}     --region=${REGION}
`

Output is as follows:

```
Job id : 2c8b7f8255064e4384786eeff495d4c2
Start time:
Dec 29, 2020, 10:53:43 PM
Elapsed time:
34 sec
Status:
Succeeded
Region
us-central1
Cluster
cluster-58d6
Job type
PySpark
Main python file
gs://dataproc-staging-us-central1-919962122668-znlnaxj2/google-cloud-dataproc-metainfo/82d82cdf-c44b-42e4-b234-a1a3f10f1194/jobs/2c8b7f8255064e4384786eeff495d4c2/staging/upload_parquet.py


20/12/29 17:23:47 INFO org.spark_project.jetty.util.log: Logging initialized @3168ms
20/12/29 17:23:47 INFO org.spark_project.jetty.server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
20/12/29 17:23:47 INFO org.spark_project.jetty.server.Server: Started @3259ms
20/12/29 17:23:47 WARN org.apache.spark.util.Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
20/12/29 17:23:47 INFO org.spark_project.jetty.server.AbstractConnector: Started ServerConnector@7722560c{HTTP/1.1,[http/1.1]}{0.0.0.0:4041}
20/12/29 17:23:47 WARN org.apache.spark.scheduler.FairSchedulableBuilder: Fair Scheduler configuration file not found so jobs will be scheduled in FIFO order. To use fair scheduling, configure pools in fairscheduler.xml or set spark.scheduler.allocation.file to a file that contains the configuration.
20/12/29 17:23:48 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-58d6-m/10.128.0.4:8032
20/12/29 17:23:49 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at cluster-58d6-m/10.128.0.4:10200
20/12/29 17:23:51 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1609238226215_0016
root
 |-- registration_dttm: timestamp (nullable = true)
 |-- id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- ip_address: string (nullable = true)
 |-- cc: string (nullable = true)
 |-- country: string (nullable = true)
 |-- birthdate: string (nullable = true)
 |-- salary: double (nullable = true)
 |-- title: string (nullable = true)
 |-- comments: string (nullable = true)

+--------------+------------------+
|       country|       avg(salary)|
+--------------+------------------+
|        Russia|146614.46322033898|
|      Paraguay|         256483.86|
|         Yemen|         139622.57|
|        Sweden| 163599.2443478261|
|      Kiribati|         235606.76|
|   Philippines|153075.90404761903|
|Norfolk Island|         248316.41|
|      Malaysia|155694.33285714284|
|      "Bonaire|              null|
|        Turkey|         166409.44|
|       Germany|       149163.0525|
|   Afghanistan|121201.01999999999|
|   Ivory Coast|         215604.11|
|         Sudan|        160131.925|
|        France|144113.54638888885|
|        Greece|174932.36666666667|
|     Sri Lanka|          78930.92|
|     Argentina| 124697.5223076923|
|       Belgium|         208938.05|
|        Angola|          229095.6|
+--------------+------------------+
only showing top 20 rows

+--------------+-----+
|       country|count|
+--------------+-----+
|        Russia|   62|
|      Paraguay|    1|
|         Yemen|    5|
|        Sweden|   25|
|      Kiribati|    1|
|   Philippines|   45|
|Norfolk Island|    1|
|      Malaysia|    8|
|      "Bonaire|    1|
|        Turkey|    1|
|       Germany|    4|
|   Afghanistan|    2|
|   Ivory Coast|    2|
|         Sudan|    2|
|        France|   37|
|        Greece|    7|
|     Sri Lanka|    2|
|     Argentina|   13|
|       Belgium|    1|
|        Angola|    1|
+--------------+-----+
only showing top 20 rows

+--------------------+---------+----------+---------+
|             country|last_name|first_name|   salary|
+--------------------+---------+----------+---------+
|           Indonesia|   Jordan|    Amanda| 49756.53|
|              Canada|  Freeman|    Albert|150280.17|
|              Russia|   Morgan|    Evelyn|144972.51|
|               China|    Riley|    Denise| 90263.05|
|           Indonesia|    White|   Kathryn| 69227.11|
|            Portugal|   Holmes|    Samuel| 14247.62|
|Bosnia and Herzeg...|   Howell|     Harry|186469.43|
|         South Korea|   Foster|      Jose|231067.84|
|             Nigeria|  Stewart|     Emily| 27234.28|
|              Russia|  Perkins|     Susan|210001.95|
|               China|    Berry|     Alice| 22944.53|
|              Zambia|    Berry|    Justin| 44165.46|
|Bosnia and Herzeg...| Reynolds|     Kathy|286592.99|
|               Japan|   Hudson|   Dorothy|157099.71|
|              Brazil|   Willis|     Bruce|239100.65|
|              Russia|  Andrews|     Emily|116800.65|
|             Ukraine|  Wallace|   Stephen|248877.99|
|              Russia|   Lawson|  Clarence|177122.99|
|               China|     Bell|   Rebecca|137251.19|
|              Russia|  Stevens|     Diane| 87978.22|
+--------------------+---------+----------+---------+
only showing top 20 rows

20/12/29 17:24:12 INFO org.spark_project.jetty.server.AbstractConnector: Stopped Spark@7722560c{HTTP/1.1,[http/1.1]}{0.0.0.0:4041}
Job output is complete
```
