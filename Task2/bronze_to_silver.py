import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

def clean_text_columns(df):
    string_columns = [field.name for field in df.schema.fields if field.dataType.typeName() == 'string']
    for column in string_columns:
        df = df.withColumn(column, trim(col(column)))
    return df

def main():
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()

    PROJECT_PATH = "/home/marichka/datalake_project"
    bronze_dir = os.path.join(PROJECT_PATH, "data/bronze")
    silver_dir = os.path.join(PROJECT_PATH, "data/silver")

    os.makedirs(silver_dir, exist_ok=True)

    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        print(f"Обробка {table}: Bronze -> Silver...")
        # Використовуємо os.path.join для надійності
        input_path = os.path.join(bronze_dir, table)
        output_path = os.path.join(silver_dir, table)

        df = spark.read.parquet(input_path)
        df = clean_text_columns(df)
        df = df.dropDuplicates()

        df.write.mode("overwrite").parquet(output_path)
        print(f"Таблицю {table} збережено в Silver: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()