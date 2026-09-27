from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, TimestampType

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

TOPIC_NAME_IN = 'student.topic.cohort16.alex'
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}

schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", DoubleType(), True),
    StructField("adv_campaign_datetime_end", DoubleType(), True),
    StructField("datetime_created", DoubleType(), True),
])

restaurant_read_stream_df = (spark.readStream 
    .format('kafka') 
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') 
    .options(**kafka_security_options) 
    .option('subscribe', TOPIC_NAME_IN) 
    .load()
    .withColumn('value', f.col('value').cast(StringType()))
    .withColumn('event', f.from_json(f.col('value'), schema))
    .selectExpr("event.*")
    .withColumn('adv_campaign_datetime_start', f.col('adv_campaign_datetime_start').cast(TimestampType()))
    .withColumn('adv_campaign_datetime_end', f.col('adv_campaign_datetime_end').cast(TimestampType()))
    .withColumn('datetime_created', f.col('datetime_created').cast(TimestampType()))
    .where((f.current_timestamp() < f.col("adv_campaign_datetime_end"))
           & (f.current_timestamp() > f.col("adv_campaign_datetime_start")))
    )

df = (
        spark.read
        .format('jdbc')
        .option('url', 'jdbc:postgresql://localhost:5432/de')
        .option('driver', 'org.postgresql.Driver')
        .option('user', 'jovyan')
        .option('password', 'jovyan')
        .option('dbtable', 'public.subscribers_restaurants')
        .load()
        )


query = (restaurant_read_stream_df.join(df, f.col("restaurant_id"))
         .dropDuplicates()
         .withColumn("current_date", f.current_date())
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .start())
query.awaitTermination()
