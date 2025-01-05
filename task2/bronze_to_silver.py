from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import os
from constants import BRONZE_PATH, SILVER_PATH


def clean_text_columns(df):
    for col_name in df.columns:
        if df.schema[col_name].dataType.typeName() == "string":
            df = df.withColumn(
                col_name, regexp_replace(col(col_name), r"^\s+|\s+$", "")
            )
    return df


def process_table(spark, table):
    bronze_table_path = os.path.join(BRONZE_PATH, table)
    silver_table_path = os.path.join(SILVER_PATH, table)
    df = spark.read.parquet(bronze_table_path)
    df = clean_text_columns(df)
    df = df.dropDuplicates()

    df.show()

    df.write.mode("overwrite").parquet(silver_table_path)

    print(f"Processed table {table} to {silver_table_path}")


def main():
    spark = SparkSession.builder.appName("Bronze to Silver").getOrCreate()

    os.makedirs(SILVER_PATH, exist_ok=True)

    bronze_tables = os.listdir(BRONZE_PATH)

    for table in bronze_tables:
        process_table(spark, table)

    spark.stop()


if __name__ == "__main__":
    main()
