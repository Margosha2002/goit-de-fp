import requests
from pyspark.sql import SparkSession
import os
from constants import BASE_URL, DATA_PATH, BRONZE_PATH, TABLES


def download_file(local_file_path):
    downloading_url = os.path.join(BASE_URL, local_file_path + ".csv")
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(os.path.join(DATA_PATH, local_file_path + ".csv"), "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


def download_spark(spark, local_file_path):
    downloading_url = os.path.join(DATA_PATH, local_file_path) + ".csv"

    df = spark.read.csv(downloading_url, header=True, inferSchema=True)

    df.show()

    bronze_table_path = os.path.join(BRONZE_PATH, local_file_path)
    df.write.mode("overwrite").parquet(bronze_table_path)
    print(f"Saved table {local_file_path} to {bronze_table_path}")


def main():
    spark = SparkSession.builder.appName("Landing to Bronze").getOrCreate()

    os.makedirs(DATA_PATH, exist_ok=True)
    os.makedirs(BRONZE_PATH, exist_ok=True)

    for table in TABLES:
        download_file(table)
        download_spark(spark, table)

    spark.stop()


if __name__ == "__main__":
    main()
