from pyspark.sql import SparkSession
from configs import mysql_options # Імпортуємо тільки SQL

spark = SparkSession.builder \
    .appName("OlympicDataProducer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,mysql:mysql-connector-java:8.0.28") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=/dev/null") \
    .getOrCreate()

# Конфіг Кафки прямо тут для наочності
kafka_options = {
    "kafka.bootstrap.servers": "77.81.230.104:9092",
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";'
}

# Цей рядок каже базі: "Дай мені тільки 1000 рядків і все"
fast_query = "(SELECT * FROM athlete_event_results LIMIT 1000) AS tiny_table"

print("Запитуємо дані у MySQL (це буде швидко)...")
df = spark.read.format("jdbc") \
    .options(**mysql_options) \
    .option("dbtable", fast_query) \
    .load()

print("Відправляємо в Кафку...")
df.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .options(**kafka_options) \
    .option("topic", "athlete_event_results") \
    .save()

print("Успішно! Кафка наповнена.")
spark.stop()