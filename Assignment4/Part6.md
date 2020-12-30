
# Running ELT process to cleanup NYC TAXI DATASET

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


Wrote the following script to perform cleanup as follows: \
1.Dropped duplicate rows \
2.Dropped rows where the trip distance is 0 \
3.filtered rows where fare is <= $2.5 which the min fare amount in NYC

```
 
import pyspark
import sys


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('nyc-taxi-app').getOrCreate()

#nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020", header=True, inferSchema=True)
nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020/yellow_tripdata_2019-01.csv", header=True, inferSchema=True)

#drop Duplicate rows
nyctaxi_df = nyctaxi_df.dropDuplicates()
print("There are {} rows in the Dataframe after dropping duplicates.".format(nyctaxi_df.count()))

nyctaxi_df.describe().show()


#Drop rows where Pick and Drop location is null
nyctaxi_df = nyctaxi_df.dropna(subset=['PULocationID', 'DOLocationID'])
print("There are {} rows in the Dataframe after dropping null location data.".format(nyctaxi_df.count()))


#Drop rows where the trip distance is 0
nyctaxi_df = nyctaxi_df.filter(nyctaxi_df.trip_distance != 0)
print("There are {} rows in the Dataframe after dropping trip_distance ==0 .".format(nyctaxi_df.count()))


#filter rows where fare is <= $2.5 which the min fare amount in NYC
nyctaxi_df = nyctaxi_df.filter(nyctaxi_df.total_amount > 2.5)
print("There are {} rows in the Dataframe after filter total fare <=2.5.".format(nyctaxi_df.count()))


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

```
Ran the job using the following command:

`gcloud dataproc jobs submit pyspark elt_upload.py --cluster=${CLUSTER} --region=${REGION}
`

Output is as follows:

```
Job Id d965580930484619ace23778dc7090f9

20/12/30 13:41:56 INFO org.spark_project.jetty.util.log: Logging initialized @3303ms
20/12/30 13:41:56 INFO org.spark_project.jetty.server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
20/12/30 13:41:56 INFO org.spark_project.jetty.server.Server: Started @3399ms
20/12/30 13:41:56 INFO org.spark_project.jetty.server.AbstractConnector: Started ServerConnector@232e9fcd{HTTP/1.1,[http/1.1]}{0.0.0.0:34097}
20/12/30 13:41:58 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-58d6-m/10.128.0.8:8032
20/12/30 13:41:58 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at cluster-58d6-m/10.128.0.8:10200
20/12/30 13:41:58 INFO org.apache.hadoop.conf.Configuration: resource-types.xml not found
20/12/30 13:41:58 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Unable to find 'resource-types.xml'.
20/12/30 13:41:58 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = memory-mb, units = Mi, type = COUNTABLE
20/12/30 13:41:58 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = vcores, units = , type = COUNTABLE
20/12/30 13:42:00 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1609329962763_0004
There are 7667792 rows in the Dataframe after dropping duplicates.
+-------+------------------+------------------+-----------------+------------------+------------------+-----------------+------------------+-------------------+------------------+------------------+-------------------+------------------+------------------+---------------------+------------------+--------------------+
|summary|          VendorID|   passenger_count|    trip_distance|        RatecodeID|store_and_fwd_flag|     PULocationID|      DOLocationID|       payment_type|       fare_amount|             extra|            mta_tax|        tip_amount|      tolls_amount|improvement_surcharge|      total_amount|congestion_surcharge|
+-------+------------------+------------------+-----------------+------------------+------------------+-----------------+------------------+-------------------+------------------+------------------+-------------------+------------------+------------------+---------------------+------------------+--------------------+
|  count|           7667792|           7667792|          7667792|           7667792|           7667792|          7667792|           7667792|            7667792|           7667792|           7667792|            7667792|           7667792|           7667792|              7667792|           7667792|             2811814|
|   mean|1.6367752281230372|1.5670782410373156|2.801083849170658|1.0583713016732847|              null|165.5009177348577|163.75290553004047| 1.2917761723322698|12.409408840250235|0.3280394473402512| 0.4968458208047375|1.8273670373427184|0.3169187035850822|   0.2993382449601017|15.682222159911758|3.289691281144485E-5|
| stddev|  0.53982043234949|1.2244306152539495|3.737529402987089|0.6780888992871958|              null|66.39179993938751| 70.36445185659788|0.47332290762047075|262.07205829471206|0.5074789117423468|0.05337843445862466|2.5012128626092234| 2.023665494717224| 0.019117114637454583| 262.2931600217453|0.009068695003703643|
|    min|                 1|                 0|              0.0|                 1|                 N|                1|                 1|                  1|            -362.0|             -60.0|               -0.5|             -63.5|             -70.0|                 -0.3|            -362.8|                 0.0|
|    max|                 4|                 9|            831.8|                99|                 Y|              265|               265|                  4|         623259.86|            535.38|               60.8|            787.25|            3288.0|                  0.6|         623261.66|                 2.5|
+-------+------------------+------------------+-----------------+------------------+------------------+-----------------+------------------+-------------------+------------------+------------------+-------------------+------------------+------------------+---------------------+------------------+--------------------+

There are 7667792 rows in the Dataframe after dropping null location data.
There are 7613022 rows in the Dataframe after dropping trip_distance ==0 .
There are 7606287 rows in the Dataframe after filter total fare <=2.5.
+------------------+
|avg(trip_distance)|
+------------------+
| 2.822219590189009|
+------------------+

+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
|       1| 2019-01-01 00:32:36|  2019-01-01 00:37:43|              1|          0.7|         1|                 Y|         239|         142|           2|        5.5|  0.5|    0.5|       0.0|         0.0|                  0.3|         6.8|                null|
|       1| 2019-01-01 00:10:27|  2019-01-01 00:17:35|              1|          2.0|         1|                 N|         140|         233|           2|        8.0|  0.5|    0.5|       0.0|         0.0|                  0.3|         9.3|                null|
|       2| 2019-01-01 00:30:51|  2019-01-01 00:41:14|              1|         0.96|         1|                 N|         148|         232|           2|        8.0|  0.5|    0.5|       0.0|         0.0|                  0.3|         9.3|                null|
|       2| 2019-01-01 00:11:54|  2019-01-01 00:19:15|              2|         1.88|         1|                 N|         114|          87|           1|        8.0|  0.5|    0.5|      2.32|         0.0|                  0.3|       11.62|                null|
|       1| 2019-01-01 00:45:36|  2019-01-01 00:57:23|              1|          2.7|         1|                 N|         264|         264|           2|       11.5|  0.5|    0.5|       0.0|         0.0|                  0.3|        12.8|                null|
|       2| 2019-01-01 00:18:26|  2019-01-01 00:38:13|              1|         6.17|         1|                 N|         223|         262|           1|       20.5|  0.5|    0.5|      4.36|         0.0|                  0.3|       26.16|                null|
|       2| 2019-01-01 00:14:16|  2019-01-01 00:25:16|              1|         1.87|         1|                 N|         161|         140|           1|        9.5|  0.5|    0.5|      2.16|         0.0|                  0.3|       12.96|                null|
|       2| 2019-01-01 01:00:00|  2019-01-01 01:10:40|              1|         2.61|         1|                 N|         263|          41|           1|       10.5|  0.5|    0.5|      2.36|         0.0|                  0.3|       14.16|                null|
|       2| 2019-01-01 00:14:41|  2019-01-01 00:35:16|              1|         5.88|         1|                 N|         229|         129|           2|       20.5|  0.5|    0.5|       0.0|         0.0|                  0.3|        21.8|                null|
|       1| 2019-01-01 00:01:08|  2019-01-01 00:19:15|              1|          7.0|         1|                 N|         140|         217|           2|       21.5|  0.5|    0.5|       0.0|         0.0|                  0.3|        22.8|                null|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
only showing top 10 rows

+---------------+-------+
|passenger_count|  count|
+---------------+-------+
|              9|      1|
|              8|     10|
|              7|      7|
|              6| 199932|
|              5| 322362|
|              4| 139881|
|              3| 312930|
|              2|1106554|
|              1|5409043|
|              0| 115567|
+---------------+-------+

+---------------+------------------+
|passenger_count|avg(trip_distance)|
+---------------+------------------+
|              1|2.8017050872030413|
|              6|2.8545413940739843|
|              3|2.8555493560860254|
|              5| 2.878454284313909|
|              9|             13.38|
|              4|2.8688649637906503|
|              8|             9.114|
|              7| 6.952857142857142|
|              2|2.8985798885549205|
|              0|2.6908660776865374|
+---------------+------------------+

+---------------+-------+
|passenger_count|  count|
+---------------+-------+
|              9|      1|
|              8|     10|
|              7|      7|
|              6| 199932|
|              5| 322362|
|              4| 139881|
|              3| 312930|
|              2|1106554|
|              1|5409043|
|              0| 115567|
+---------------+-------+

20/12/30 14:05:23 INFO org.spark_project.jetty.server.AbstractConnector: Stopped Spark@232e9fcd{HTTP/1.1,[http/1.1]}{0.0.0.0:0}
Job output is complete
```
