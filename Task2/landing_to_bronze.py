import os
import requests
from pyspark.sql import SparkSession

def download_data(url, local_path):
    print(f"Завантаження {url}...")
    response = requests.get(url)
    response.raise_for_status()  # Перевірка на помилки
    with open(local_path, 'wb') as f:
        f.write(response.content)
    print(f"Збережено в {local_path}")

def main():
    # 1. Ініціалізація Spark
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .getOrCreate()

    # Вказуємо АБСОЛЮТНИЙ шлях до папки проекту
    PROJECT_PATH = "/home/marichka/datalake_project"
    
    # Створюємо повні шляхи до папок
    landing_dir = os.path.join(PROJECT_PATH, "data/landing")
    bronze_dir = os.path.join(PROJECT_PATH, "data/bronze")

    # Переконуємося, що папки існують
    os.makedirs(landing_dir, exist_ok=True)
    os.makedirs(bronze_dir, exist_ok=True)

    base_url = "https://ftp.goit.study/neoversity/"
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        csv_url = f"{base_url}{table}.csv"
        local_csv_path = os.path.join(landing_dir, f"{table}.csv")
        
        # 2. Завантаження (Landing Zone)
        download_data(csv_url, local_csv_path)

        # 3. Читання CSV та запис у Bronze (Parquet)
        print(f"Обробка таблиці {table}...")
        df = spark.read.csv(local_csv_path, header=True, inferSchema=True)
        df.show(10)
        
        output_path = os.path.join(bronze_dir, table)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Таблицю {table} успішно збережено в Bronze (Parquet) за шляхом: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()