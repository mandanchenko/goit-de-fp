from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from configs import mysql_options

# Створюємо сесію Spark
spark = SparkSession.builder \
    .appName("MarichkaOlympicStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,mysql:mysql-connector-java:8.0.28") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=/dev/null") \
    .getOrCreate()

# Етап 1 та 2. Зчитування та фільтрація біо-даних 
print("Зчитуємо дані з MySQL...")
bio_df = spark.read.format("jdbc").options(**mysql_options).option("dbtable", "athlete_bio").load()

bio_df_cleaned = bio_df.filter(
    col("height").isNotNull() & (col("height") > 0) &
    col("weight").isNotNull() & (col("weight") > 0)
).select("athlete_id", "height", "weight")

# Етап 3. Kafka з авторизацією 
kafka_options = {
    "kafka.bootstrap.servers": "77.81.230.104:9092",
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    "subscribe": "athlete_event_results",
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "5000",       # Спарк буде брати по 5000 повідомлень за раз
    "kafkaConsumer.pollTimeoutMs": "300000" # Збільшуємо час очікування до 5 хвилин
}

raw_stream = spark.readStream.format("kafka").options(**kafka_options).load()

schema = StructType([
    StructField("athlete_id", IntegerType()),
    StructField("sport", StringType()),
    StructField("medal", StringType()),
    StructField("sex", StringType()),
    StructField("country_noc", StringType())
])

events_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# Етап 4. Об’єднання даних (Join)
enriched_df = events_df.join(bio_df_cleaned, "athlete_id")

# Етап 5. Агрегація
aggregated_df = enriched_df.groupBy(
    "sport", "medal", "sex", "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
).withColumn("timestamp", current_timestamp())

# Етап 6. Функція для запису Batch
def process_batch(batch_df, batch_id):
    print(f"--- Processing Batch {batch_id} ---")
    batch_df.show(truncate=False) # Це виконає вимогу "виведення на екран"
    
    # Етап 6.а) Запис у вихідний Kafka-топік
    batch_df.select(to_json(struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .options(**kafka_options) \
        .option("topic", "marichka_enriched_results") \
        .save()
    
    # Етап 6.b) Запис у базу даних MySQL
    batch_df.write.format("jdbc") \
        .options(**mysql_options) \
        .option("dbtable", "marichka_streaming_results") \
        .mode("append") \
        .save()

# Етап 6. Запуск єдиного фінального стріму
query = aggregated_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

query.awaitTermination()