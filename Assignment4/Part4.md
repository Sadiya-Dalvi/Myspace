
# Reading semistructured (JSON) data using pyspark and submitting the job

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

Wrote the following script to read json file from google cloud storage 

```
import pyspark
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('UNESE Data').getOrCreate()

path = "gs://dataproc-nyc-taxi-2020/unece.json"
df = spark.read.json(path)

#print UN Data schema
df.printSchema()

# Mean age of women at first childbirth by Country 
df.groupby('Country').agg({'Mean age of women at birth of first child': 'mean'}).orderBy(df.Country.desc()).show()

```
Ran the job using the following command:

`gcloud dataproc jobs submit pyspark upload_json.py     --cluster=${CLUSTER}     --region=${REGION}
`

Output is as follows:

```
Job Id: 5db61abb2c6743dfb9b3d376e8d37289
20/12/30 08:26:18 INFO org.spark_project.jetty.util.log: Logging initialized @2867ms
20/12/30 08:26:18 INFO org.spark_project.jetty.server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
20/12/30 08:26:18 INFO org.spark_project.jetty.server.Server: Started @2950ms
20/12/30 08:26:18 INFO org.spark_project.jetty.server.AbstractConnector: Started ServerConnector@7d28ad01{HTTP/1.1,[http/1.1]}{0.0.0.0:35019}
20/12/30 08:26:19 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-58d6-m/10.128.0.6:8032
20/12/30 08:26:19 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at cluster-58d6-m/10.128.0.6:10200
20/12/30 08:26:20 INFO org.apache.hadoop.conf.Configuration: resource-types.xml not found
20/12/30 08:26:20 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Unable to find 'resource-types.xml'.
20/12/30 08:26:20 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = memory-mb, units = Mi, type = COUNTABLE
20/12/30 08:26:20 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = vcores, units = , type = COUNTABLE
20/12/30 08:26:21 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1609311745804_0002
root
 |-- Adolescent fertility rate: double (nullable = true)
 |-- Area (square kilometres): long (nullable = true)
 |-- Computer use, 16-24, female: double (nullable = true)
 |-- Computer use, 16-24, male: double (nullable = true)
 |-- Computer use, 25-54, female: double (nullable = true)
 |-- Computer use, 25-54, male: double (nullable = true)
 |-- Computer use, 55-74, female: double (nullable = true)
 |-- Computer use, 55-74, male: double (nullable = true)
 |-- Consumer price index, growth rate: double (nullable = true)
 |-- Country: string (nullable = true)
 |-- Economic acivity rate, women 15-64: double (nullable = true)
 |-- Economic activity rate, men 15-64: double (nullable = true)
 |-- Employment in agriculture, hunting, forestry and fishing (ISIC Rev. 4 A), share of total employment: double (nullable = true)
 |-- Employment in construction (ISIC Rev. 4 F), share of total employment: double (nullable = true)
 |-- Employment in finance, real estate and business services (ISIC Rev. 4 K-N), share of total employment: double (nullable = true)
 |-- Employment in industry and energy (ISIC Rev. 4 B-E), share of total employment: double (nullable = true)
 |-- Employment in other service activities (ISIC Rev. 4 R-U), share of total employment: double (nullable = true)
 |-- Employment in public administration, education and health (ISIC Rev. 4 O-Q), share of total employment: double (nullable = true)
 |-- Employment in trade, hotels, restaurants, transport and communications (ISIC Rev. 4 G-J), share of total employment: double (nullable = true)
 |-- Exchange rate (XR), NCU per US$: double (nullable = true)
 |-- Export of goods and services, per cent of GDP: double (nullable = true)
 |-- External balance on goods and services, per cent of GDP: double (nullable = true)
 |-- Female ambassadors, percent of total: double (nullable = true)
 |-- Female clerks, percent of total for both sexes: double (nullable = true)
 |-- Female craft and related workers, percent of total for both sexes: double (nullable = true)
 |-- Female government ministers, percent of total: double (nullable = true)
 |-- Female judges, percent of total: double (nullable = true)
 |-- Female legislators, senior officials and managers, percent of total: double (nullable = true)
 |-- Female members of parliament, percent of total: double (nullable = true)
 |-- Female part-time employment, percent of both sexes: double (nullable = true)
 |-- Female plant and machine operators and assemblers, percent of total for both sexes: double (nullable = true)
 |-- Female professionals, percent of total for both sexes: double (nullable = true)
 |-- Female tertiary students, percent of total: double (nullable = true)
 |-- Final consumption expenditure per capita, US Dollars, current PPPs: double (nullable = true)
 |-- GDP at current prices and PPPs, millions of US$: double (nullable = true)
 |-- GDP at current prices, millions of NCUs: double (nullable = true)
 |-- GDP in agriculture (ISIC4 A): output approach, index, 2005=100: double (nullable = true)
 |-- GDP in industry (incl. construction) (ISIC4 B-F): output approach, index, 2005=100: double (nullable = true)
 |-- GDP in services (ISIC4 G-U): output approach, index, 2005=100: double (nullable = true)
 |-- GDP per capita at current prices and PPPs, US$: double (nullable = true)
 |-- GDP per capita at current prices, NCUs: double (nullable = true)
 |-- GDP: in agriculture etc. (ISIC4 A), output approach, per cent share of GVA: double (nullable = true)
 |-- GDP: in construction (ISIC4 F), output approach, per cent share of GVA: double (nullable = true)
 |-- GDP: in finance and business services (ISIC4 K-N), output approach, per cent share of GVA: double (nullable = true)
 |-- GDP: in industry etc. (ISIC4 B-E), output approach, per cent share of GVA: double (nullable = true)
 |-- GDP: in other service activities (ISIC4 R-U), output approach, per cent share of GVA: double (nullable = true)
 |-- GDP: in public administration, education and health (ISIC4 O-Q), output approach, per cent share of GVA: double (nullable = true)
 |-- GDP: in trade, hospitality, transport and communication (ISIC4 G-J), output approach, per cent share of GVA: double (nullable = true)
 |-- Gender pay gap in hourly earning wage rate: double (nullable = true)
 |-- Gender pay gap in monthly earnings: double (nullable = true)
 |-- Import of goods and services, per cent of GDP: double (nullable = true)
 |-- Life expectancy at age 65, men: double (nullable = true)
 |-- Life expectancy at age 65, women: double (nullable = true)
 |-- Life expectancy at birth, men: double (nullable = true)
 |-- Life expectancy at birth, women: double (nullable = true)
 |-- Mean age of women at birth of first child: double (nullable = true)
 |-- Persons injured in road accidents: long (nullable = true)
 |-- Persons killed in road accidents: long (nullable = true)
 |-- Population aged 0-14, female: double (nullable = true)
 |-- Population aged 0-14, male: double (nullable = true)
 |-- Population aged 15-64, female: double (nullable = true)
 |-- Population aged 15-64, male: double (nullable = true)
 |-- Population aged 64+, female: double (nullable = true)
 |-- Population aged 64+, male: double (nullable = true)
 |-- Population density, pers. per sq. km: double (nullable = true)
 |-- Purchasing power parity (PPP), NCU per US$: double (nullable = true)
 |-- Total employment, growth rate: double (nullable = true)
 |-- Total fertility rate: double (nullable = true)
 |-- Total length of motorways (km): double (nullable = true)
 |-- Total length of railway lines (km): double (nullable = true)
 |-- Total population: double (nullable = true)
 |-- Total population, female (%): double (nullable = true)
 |-- Total population, male (%): double (nullable = true)
 |-- Unemployment rate: double (nullable = true)
 |-- Women Researchers, Percent of corresponding total for both sexes: double (nullable = true)
 |-- Women in the Labour Force, Percent of corresponding total for both sexes: double (nullable = true)
 |-- Women, percent of all victims of homicides: double (nullable = true)
 |-- Year: string (nullable = true)
 |-- Youth unemployment rate: double (nullable = true)

20/12/30 08:26:36 WARN org.apache.spark.util.Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.
+--------------------+----------------------------------------------+
|             Country|avg(Mean age of women at birth of first child)|
+--------------------+----------------------------------------------+
|          Uzbekistan|                            23.419999999999998|
|       United States|                                          25.4|
|      United Kingdom|                                         27.44|
|             Ukraine|                            23.499999999999996|
|        Turkmenistan|                            24.428571428571423|
|              Turkey|                                          22.3|
|The former Yugosl...|                             25.44666666666667|
|          Tajikistan|                             22.37333333333333|
|         Switzerland|                            29.735069333333335|
|              Sweden|                                      28.86875|
|               Spain|                                     29.673055|
|            Slovenia|                            28.040000000000003|
|            Slovakia|                            26.113333333333337|
|              Serbia|                             26.41333333333333|
|  Russian Federation|                                        24.043|
|             Romania|                            25.213333333333328|
|            Portugal|                            28.362499999999997|
|              Poland|                            25.972666666666665|
|              Norway|                                        28.125|
|         Netherlands|                            29.368749999999995|
+--------------------+----------------------------------------------+
only showing top 20 rows

20/12/30 08:26:41 INFO org.spark_project.jetty.server.AbstractConnector: Stopped Spark@7d28ad01{HTTP/1.1,[http/1.1]}{0.0.0.0:0}
Job output is complete
```
