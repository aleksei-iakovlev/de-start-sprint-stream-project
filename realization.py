from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType

TOPIC_NAME_IN = 'student.topic.cohort16.alex'
TOPIC_NAME_OUT = 'student.topic.cohort16.alex.out'
CHECKPOINT_LOCATION = 'checkpoints/restaurant_subscribe'
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}
postgres_security_options = {
    'user': 'jovyan',
    'password': 'jovyan'
}


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.cache()

    try:
        df_out = df.drop("event_time")
        # в Postgres не пишем id подписчика: это PK serial в subscribers_feedback
        df_feedback = df_out.drop("id").withColumn("feedback", f.lit(None).cast(StringType()))
        (df_feedback.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/de")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", "public.subscribers_feedback")
            .options(**postgres_security_options)
            .mode("append")
            .save())
        # в Kafka id подписчика остаётся в json
        kafka_df = df_out.select(
            f.col("restaurant_id").cast("string").alias("key"),
            f.to_json(f.struct(*[f.col(c) for c in df_out.columns])).alias("value"))
        # отправляем сообщения в результирующий топик Kafka без поля feedback
        (kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
            .option("topic", TOPIC_NAME_OUT)
            .options(**kafka_security_options)
            .save())
    # очищаем память от df
    finally:
        df.unpersist()


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = (spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
    .options(**kafka_security_options) 
    .option('subscribe', TOPIC_NAME_IN)
    .load())

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = f.unix_timestamp()

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (restaurant_read_stream_df
                           .withColumn('value', f.col('value').cast(StringType()))
                           .withColumn('event', f.from_json(f.col('value'), incomming_message_schema))
                           .selectExpr("event.*")
                           .where((current_timestamp_utc < f.col("adv_campaign_datetime_end"))
                                  & (current_timestamp_utc > f.col("adv_campaign_datetime_start")))
                           .withColumn("event_time", f.col("datetime_created").cast("timestamp"))
                           .withWatermark("event_time", "1 hour")
                           )

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = (spark.read
                                .format('jdbc')
                                .option('url', 'jdbc:postgresql://localhost:5432/de')
                                .option('driver', 'org.postgresql.Driver')
                                .option('user', 'jovyan')
                                .option('password', 'jovyan')
                                .option('dbtable', 'public.subscribers_restaurants')
                                .load())

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = (filtered_read_stream_df.join(subscribers_restaurant_df, "restaurant_id")
             .dropDuplicates(["client_id", "restaurant_id", "adv_campaign_id"])
             .withColumn("trigger_datetime_created", f.unix_timestamp().cast("int")))

# запускаем стриминг
result_df.writeStream \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
