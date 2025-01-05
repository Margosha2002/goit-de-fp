import os
from pyspark.sql import SparkSession
from helpers.get_athlete_bio import get_athlete_bio
from helpers.parse_results import parse_results
from helpers.read_from_kafka import read_from_kafka
from helpers.aggregate_athletes import aggregate_athletes
from helpers.write_to_kafka import write_stream_to_kafka
from helpers.write_to_mysql import write_stream_to_mysql


os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

spark = (
    SparkSession.builder.appName("JDBCToKafka")
    .config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .getOrCreate()
)
# Етап 3.1: Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results
results_df = read_from_kafka(spark, "athlete_event_results")

results_df = parse_results(results_df)
bio_df = get_athlete_bio(spark)

aggregated_df = aggregate_athletes(results_df, bio_df)
# Етап 6.1): запис у вихідний Kafka-топік
aggregated_df.writeStream.foreachBatch(
    write_stream_to_kafka("athlete_enriched_agg")
).outputMode("update").start()
# Етап 6.2): запис у базу даних
aggregated_df.writeStream.foreachBatch(
    write_stream_to_mysql("athlete_enriched_agg")
).outputMode("update").start()

aggregated_df.writeStream.outputMode("update").format("console").start()


try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"Error occurred: {e}")
