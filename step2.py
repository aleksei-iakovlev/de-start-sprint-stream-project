from pyspark.sql import SparkSession

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
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .options(**kafka_security_options) \
    .option('subscribe', TOPIC_NAME_IN) \
    .load()

query = (restaurant_read_stream_df
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start())
query.awaitTermination()
