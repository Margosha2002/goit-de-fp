import os
from pyspark.sql import SparkSession
from helpers.get_athlete_results import get_athlete_results
from helpers.write_to_kafka import write_to_kafka


os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

spark = (
    SparkSession.builder.appName("JDBCToKafka")
    .config("spark.jars", "task1/mysql-connector-j-8.0.32.jar")
    .getOrCreate()
)
# Етап 3: Зчитати дані з mysql таблиці athlete_event_results
results_df = get_athlete_results(spark)
# Етап 3.1: Записати в кафка топік athlete_event_results
write_to_kafka(results_df, "athlete_event_results")

try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"Error occurred: {e}")
