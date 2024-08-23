from pyspark.sql import SparkSession

# Creating the Spark session
spark = SparkSession.builder \
    .master('yarn') \
    .appName('BigQuery to GCS') \
    .getOrCreate()

# Google Cloud Storage Demo
gcs_bucket = 'your-gs-bucket'
spark.conf.set('temporaryGcsBucket', gcs_bucket)

# load data from BigQuery (shakepeare dataset)
shakespeare = spark.read \
    .format('bigquery') \
    .option('table', 'bigquery-public-data:samples.shakespeare') \
    .load()

shakespeare.createOrReplaceTempView('shakespeare')

# Querying the data
shakespeare=spark.sql('SELECT word, word_count FROM shakespeare WHERE corpus = "hamlet"')

# Writing the data to GCS
shakespeare.write.csv(f'gs://{gcs_bucket}/shakespeare/shakespeare.csv')
