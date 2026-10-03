from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType

TOPIC_NAME_IN = 'student.topic.cohort16.alex'
TOPIC_NAME_OUT = 'student.topic.cohort16.alex.out'
CHECKPOINT_LOCATION = 'checkpoints/restaurant_subscribe'
KAFKA_BOOTSTRAP_SERVERS = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";'
}
postgres_security_options = {
    'user': 'jovyan',
    'password': 'jovyan'
}


class BatchOutputWriter:
    """Writes each batch to PostgreSQL and Kafka."""

    def write_batch(self, df, epoch_id):
        # Сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka.
        df.cache()

        try:
            df_out = df.drop("event_time")
            self._write_feedback(df_out)
            self._write_trigger(df_out)
        finally:
            # Очищаем память от df даже в случае ошибки записи.
            df.unpersist()

    @staticmethod
    def _write_feedback(df_out):
        # В Postgres не пишем id подписчика: это PK serial в subscribers_feedback.
        df_feedback = df_out.drop("id").withColumn(
            "feedback", f.lit(None).cast(StringType())
        )
        (df_feedback.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/de")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", "public.subscribers_feedback")
            .options(**postgres_security_options)
            .mode("append")
            .save())

    @staticmethod
    def _write_trigger(df_out):
        # В Kafka id подписчика остаётся в json.
        kafka_df = df_out.select(
            f.col("restaurant_id").cast("string").alias("key"),
            f.to_json(f.struct(*[f.col(column) for column in df_out.columns])).alias("value")
        )
        # Отправляем сообщения в результирующий топик Kafka без поля feedback.
        (kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", TOPIC_NAME_OUT)
            .options(**kafka_security_options)
            .save())


class RestaurantSubscribeStreamingService:
    """Builds and runs the restaurant campaign subscription stream."""

    def __init__(self, spark=None):
        self.spark = spark or self._create_spark_session()
        self.batch_output_writer = BatchOutputWriter()

    @staticmethod
    def _create_spark_session():
        # Необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL.
        spark_jars_packages = ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ])

        # Создаём Spark-сессию с нужными библиотеками и UTC timezone.
        return (SparkSession.builder
                .appName("RestaurantSubscribeStreamingService")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.jars.packages", spark_jars_packages)
                .getOrCreate())

    @staticmethod
    def _incoming_message_schema():
        # Определяем схему входного сообщения для json.
        return StructType([
            StructField("restaurant_id", StringType(), True),
            StructField("adv_campaign_id", StringType(), True),
            StructField("adv_campaign_content", StringType(), True),
            StructField("adv_campaign_owner", StringType(), True),
            StructField("adv_campaign_owner_contact", StringType(), True),
            StructField("adv_campaign_datetime_start", LongType(), True),
            StructField("adv_campaign_datetime_end", LongType(), True),
            StructField("datetime_created", LongType(), True),
        ])

    def _read_campaign_stream(self):
        # Читаем из топика Kafka сообщения с акциями от ресторанов.
        restaurant_read_stream_df = (self.spark.readStream
                                     .format("kafka")
                                     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                                     .options(**kafka_security_options)
                                     .option("subscribe", TOPIC_NAME_IN)
                                     .load())

        # Фильтруем акции по времени начала и окончания.
        current_timestamp_utc = f.unix_timestamp()
        return (restaurant_read_stream_df
                .withColumn("value", f.col("value").cast(StringType()))
                .withColumn("event", f.from_json(f.col("value"), self._incoming_message_schema()))
                .selectExpr("event.*")
                .where((current_timestamp_utc < f.col("adv_campaign_datetime_end"))
                       & (current_timestamp_utc > f.col("adv_campaign_datetime_start")))
                .withColumn("event_time", f.col("datetime_created").cast("timestamp"))
                .withWatermark("event_time", "1 hour"))

    def _read_subscribers(self):
        # Вычитываем всех пользователей с подпиской на рестораны.
        return (self.spark.read
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/de")
                .option("driver", "org.postgresql.Driver")
                .option("user", "jovyan")
                .option("password", "jovyan")
                .option("dbtable", "public.subscribers_restaurants")
                .load())

    def _build_result_stream(self):
        # Джойним кампании с подписками и добавляем время создания события.
        return (self._read_campaign_stream()
                .join(self._read_subscribers(), "restaurant_id")
                .dropDuplicates(["client_id", "restaurant_id", "adv_campaign_id"])
                .withColumn("trigger_datetime_created", f.unix_timestamp().cast("int")))

    def run(self):
        result_df = self._build_result_stream()
        (result_df.writeStream
         .option("checkpointLocation", CHECKPOINT_LOCATION)
         .foreachBatch(self.batch_output_writer.write_batch)
         .start()
         .awaitTermination())


def main():
    RestaurantSubscribeStreamingService().run()


if __name__ == "__main__":
    main()
