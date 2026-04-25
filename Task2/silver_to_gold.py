import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp

def main():
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()

    PROJECT_PATH = "/home/marichka/datalake_project"
    silver_dir = os.path.join(PROJECT_PATH, "data/silver")
    gold_dir = os.path.join(PROJECT_PATH, "data/gold/avg_stats")

    # Зчитуємо
    bio_df = spark.read.parquet(os.path.join(silver_dir, "athlete_bio"))
    results_df = spark.read.parquet(os.path.join(silver_dir, "athlete_event_results"))

    # Вирішуємо проблему з country_noc
    bio_df_cleaned = bio_df.drop("country_noc")
    combined_df = results_df.join(bio_df_cleaned, "athlete_id")

    gold_df = combined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("weight").alias("avg_weight"),
            avg("height").alias("avg_height")
        )

    gold_df = gold_df.withColumn("timestamp", current_timestamp())

    gold_df.write.mode("overwrite").parquet(gold_dir)
    print(f"Gold збережено: {gold_dir}")

    print("--- ТАБЛИЦЯ GOLD ---")
    gold_df.show(10)
    
    spark.stop()

if __name__ == "__main__":
    main()