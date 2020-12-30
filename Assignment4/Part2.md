# Reading structured data (CSV) using pyspark and submitting the job

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

Wrote the following script to read csv file from google cloud storage 

```
 import pyspark
 import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('nyc-taxi-app').getOrCreate()
#nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020", header=True, inferSchema=True)
nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020/yellow_tripdata_2019-01.csv", header=True, inferSchema=True)
nyctaxi_df.describe().show()
# Find the average trip distance 
nyctaxi_df.agg({'trip_distance': 'mean'}).show()
# show first 10 rows
output = nyctaxi_df.show(10)
# Trips by passenger count ordered by passenger count in descending order
nyctaxi_df.groupby('passenger_count').count().orderBy(nyctaxi_df.passenger_count.desc()).show()
# Average trip distance by passenger count
nyctaxi_df.groupby('passenger_count').agg({'trip_distance': 'mean'}).show()
#  Number of trips by passenger count, sorted in descending 
# order of passenger count
nyctaxi_df.groupby('passenger_count').count(). \
orderBy(nyctaxi_df.passenger_count.desc()).show()
#sc = pyspark.SparkContext()
#lines = sc.textFile(sys.argv[1])
#words = lines.flatMap(lambda line: line.split())
#wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)
#output.saveAsTextFile("gs://dataproc-nyc-taxi-2020/output.txt")

```
Ran the job using the following command:

`gcloud dataproc jobs submit pyspark upload.py     --cluster=${CLUSTER}     --region=${REGION}`

Output is as follows:

```
20/12/29 13:49:11 INFO org.spark_project.jetty.util.log: Logging initialized @4528ms
20/12/29 13:49:11 INFO org.spark_project.jetty.server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
20/12/29 13:49:11 INFO org.spark_project.jetty.server.Server: Started @4661ms
20/12/29 13:49:11 WARN org.apache.spark.util.Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
20/12/29 13:49:11 INFO org.spark_project.jetty.server.AbstractConnector: Started ServerConnector@6cf258c3{HTTP/1.1,[http/1.1]}{0.0.0.0:4041}
20/12/29 13:49:11 WARN org.apache.spark.scheduler.FairSchedulableBuilder: Fair Scheduler configuration file not found so jobs will be scheduled in FIFO order. To use fair scheduling, configure pools in fairscheduler.xml or set spark.scheduler.allocation.file to a file that contains the configuration.
20/12/29 13:49:13 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-58d6-m/10.128.0.4:8032
20/12/29 13:49:13 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at cluster-58d6-m/10.128.0.4:10200
20/12/29 13:49:16 WARN org.apache.hadoop.hdfs.DataStreamer: Caught exception
java.lang.InterruptedException
	at java.lang.Object.wait(Native Method)
	at java.lang.Thread.join(Thread.java:1252)
	at java.lang.Thread.join(Thread.java:1326)
	at org.apache.hadoop.hdfs.DataStreamer.closeResponder(DataStreamer.java:980)
	at org.apache.hadoop.hdfs.DataStreamer.endBlock(DataStreamer.java:630)
	at org.apache.hadoop.hdfs.DataStreamer.run(DataStreamer.java:807)
20/12/29 13:49:16 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1609238226215_0006
+-------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+-----------------+--------------------+
|summary|          VendorID|   passenger_count|     trip_distance|        RatecodeID|store_and_fwd_flag|     PULocationID|      DOLocationID|       payment_type|       fare_amount|             extra|            mta_tax|        tip_amount|       tolls_amount|improvement_surcharge|     total_amount|congestion_surcharge|
+-------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+-----------------+--------------------+
|  count|           7667792|           7667792|           7667792|           7667792|           7667792|          7667792|           7667792|            7667792|           7667792|           7667792|            7667792|           7667792|            7667792|              7667792|          7667792|             2811814|
|   mean|1.6367752281230372|1.5670782410373156|2.8010838491705305|1.0583713016732847|              null|165.5009177348577|163.75290553004047| 1.2917761723322698|12.409408840250283| 0.328039447340251| 0.4968458208047375|1.8273670373427788|0.31691870358536745|  0.29933824495278977|15.68222216009666|3.289691281144485E-5|
| stddev|0.5398204323494866|1.2244306152539546| 3.737529402987038|0.6780888992871946|              null|66.39179993938723|  70.3644518565979|0.47332290762046825|262.07205829471377|0.5074789117423503|0.05337843445862466|2.5012128626092256| 2.0236654947172377| 0.019117114637454326|262.2931600217461| 0.00906869500370369|
|    min|                 1|                 0|               0.0|                 1|                 N|                1|                 1|                  1|            -362.0|             -60.0|               -0.5|             -63.5|              -70.0|                 -0.3|           -362.8|                 0.0|
|    max|                 4|                 9|             831.8|                99|                 Y|              265|               265|                  4|         623259.86|            535.38|               60.8|            787.25|             3288.0|                  0.6|        623261.66|                 2.5|
+-------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-------------------+------------------+------------------+-------------------+------------------+-------------------+---------------------+-----------------+--------------------+

+------------------+
|avg(trip_distance)|
+------------------+
|2.8010838491705305|
+------------------+

+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
|       1| 2019-01-01 00:46:40|  2019-01-01 00:53:20|              1|          1.5|         1|                 N|         151|         239|           1|        7.0|  0.5|    0.5|      1.65|         0.0|                  0.3|        9.95|                null|
|       1| 2019-01-01 00:59:47|  2019-01-01 01:18:59|              1|          2.6|         1|                 N|         239|         246|           1|       14.0|  0.5|    0.5|       1.0|         0.0|                  0.3|        16.3|                null|
|       2| 2018-12-21 13:48:30|  2018-12-21 13:52:40|              3|          0.0|         1|                 N|         236|         236|           1|        4.5|  0.5|    0.5|       0.0|         0.0|                  0.3|         5.8|                null|
|       2| 2018-11-28 15:52:25|  2018-11-28 15:55:45|              5|          0.0|         1|                 N|         193|         193|           2|        3.5|  0.5|    0.5|       0.0|         0.0|                  0.3|        7.55|                null|
|       2| 2018-11-28 15:56:57|  2018-11-28 15:58:33|              5|          0.0|         2|                 N|         193|         193|           2|       52.0|  0.0|    0.5|       0.0|         0.0|                  0.3|       55.55|                null|
|       2| 2018-11-28 16:25:49|  2018-11-28 16:28:26|              5|          0.0|         1|                 N|         193|         193|           2|        3.5|  0.5|    0.5|       0.0|        5.76|                  0.3|       13.31|                null|
|       2| 2018-11-28 16:29:37|  2018-11-28 16:33:43|              5|          0.0|         2|                 N|         193|         193|           2|       52.0|  0.0|    0.5|       0.0|         0.0|                  0.3|       55.55|                null|
|       1| 2019-01-01 00:21:28|  2019-01-01 00:28:37|              1|          1.3|         1|                 N|         163|         229|           1|        6.5|  0.5|    0.5|      1.25|         0.0|                  0.3|        9.05|                null|
|       1| 2019-01-01 00:32:01|  2019-01-01 00:45:39|              1|          3.7|         1|                 N|         229|           7|           1|       13.5|  0.5|    0.5|       3.7|         0.0|                  0.3|        18.5|                null|
|       1| 2019-01-01 00:57:32|  2019-01-01 01:09:32|              2|          2.1|         1|                 N|         141|         234|           1|       10.0|  0.5|    0.5|       1.7|         0.0|                  0.3|        13.0|                null|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
only showing top 10 rows

+---------------+-------+
|passenger_count|  count|
+---------------+-------+
|              9|      9|
|              8|     29|
|              7|     19|
|              6| 200811|
|              5| 323842|
|              4| 140753|
|              3| 314721|
|              2|1114106|
|              1|5456121|
|              0| 117381|
+---------------+-------+

+---------------+------------------+
|passenger_count|avg(trip_distance)|
+---------------+------------------+
|              1| 2.779088319338911|
|              6|2.8423347326590713|
|              3|2.8406983328090676|
|              5|2.8657414109349544|
|              9|1.4866666666666668|
|              4|2.8530836287681223|
|              8|3.1427586206896545|
|              7| 2.561578947368421|
|              2| 2.880572494897258|
|              0|2.6515610703606276|
+---------------+------------------+

+---------------+-------+
|passenger_count|  count|
+---------------+-------+
|              9|      9|
|              8|     29|
|              7|     19|
|              6| 200811|
|              5| 323842|
|              4| 140753|
|              3| 314721|
|              2|1114106|
|              1|5456121|
|              0| 117381|
+---------------+-------+

20/12/29 13:54:50 INFO org.spark_project.jetty.server.AbstractConnector: Stopped Spark@6cf258c3{HTTP/1.1,[http/1.1]}{0.0.0.0:4041}
Job output is complete
```
