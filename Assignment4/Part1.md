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
<kbd>
<img src="https://github.com/Sadiya-Dalvi/SDProfile/blob/main/Images/cluster_to_Read_data_Frombq.png" alt="Create Cluster" width="700" height="300">
</kbd>


Created a bigquery dataset using the following command

`bq mk wordcount_dataset`

Created he following word_count.py to read and write data to BQ:

```
#!/usr/bin/python
"""BigQuery I/O PySpark example."""
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "nyc-taxi-data-sadiya"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
words = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data:samples.shakespeare') \
  .load()
words.createOrReplaceTempView('words')

# Perform word count.
word_count = spark.sql(
    'SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word')
word_count.show()
word_count.printSchema()

# Saving the data to BigQuery
word_count.write.format('bigquery') \
  .option('table', 'wordcount_dataset.wordcount_output') \
  .save()
  ```

Created a word_count.py file using the pre-installed nano editor and pasted the above code into it:

`nano wordcount.py`

Ran the job using the following command on the master node using SSH:

`spark-submit --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar wordcount.py`

The output is as shown in below ss:

<kbd>
<img src="https://github.com/Sadiya-Dalvi/SDProfile/blob/main/Images/bigquery-datproc-shakespeare.png" alt="bq1" width="700" height="300">
</kbd>

<kbd>
<img src="https://github.com/Sadiya-Dalvi/SDProfile/blob/main/Images/bigquery-datproc-shakespeare2.png" alt="bq2" width="700" height="300">
</kbd>

After sometime the data gets loaded in the bigquery table "wordcount_output" as shown below:

<kbd>
<img src="https://github.com/Sadiya-Dalvi/SDProfile/blob/main/Images/wordcount_data_loaded_in_bigquery.png" alt="bq-output" width="700" height="300">
</kbd>

# Uploaded data using a CSV file into Datastore

Wrote the following script to upload data to datastore

```import sys
import csv
import datetime
from google.cloud import datastore

#{
 #   "kind": "User",
  #  "UserId": 1929,
 #   "Gender": "F",
 #   "Age": 1,
 #   "Occupation": 10,
  #  "Zip-code": 91107
  #},

client = datastore.Client()

with open('users.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    entity = {}
    i = 0
    for row in csv_reader:

        entity['kind'] = 'User'
        entity['UserId'] = row[1]
        entity['Gender'] = row[2]
        entity['Age'] = row[3]
        entity['Occupation'] = row[4]
        entity['Zip-code'] = row[5]

        parent_key = None
        #parent_key = client.key("Movie", entity["MovieId"],
        #                      "Rating", entity["UserId"])

        key = client.key('User', entity['UserId'], parent=parent_key)

        datastore_ent = datastore.Entity(key)
        #print(entity)
        datastore_ent.update(entity) #Include properties+id
        client.put(datastore_ent)
        i = i + 1
        if i % 1000 == 0:
            print(i)
```
output is as below:

<kbd>
<img src="https://github.com/Sadiya-Dalvi/SDProfile/blob/main/Images/datastore1.png" alt="bq2" width="700" height="300">
</kbd>

Wrote a simple query:

```
import sys
import csv
import datetime
from google.cloud import datastore

client = datastore.Client()

query = client.query(kind='User')

query_iter = query.fetch()
for entity in query_iter:
    print(entity)
 ```
output is as below:

<kbd>
<img src="https://github.com/Sadiya-Dalvi/SDProfile/blob/main/Images/datastore2.png" alt="bq2" width="700" height="300">
</kbd>
