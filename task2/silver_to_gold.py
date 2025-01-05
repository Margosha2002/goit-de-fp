from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os
from constants import SILVER_PATH, GOLD_PATH, TABLES


def main():
    spark = SparkSession.builder.appName("Silver to Gold").getOrCreate()

    os.makedirs(GOLD_PATH, exist_ok=True)

    athlete_bio = spark.read.parquet(os.path.join(SILVER_PATH, TABLES[0]))
    athlete_event_results = spark.read.parquet(os.path.join(SILVER_PATH, TABLES[1]))

    joined_df = athlete_event_results.join(athlete_bio, "athlete_id")

    result_df = (
        joined_df.groupBy(
            athlete_event_results["sport"],
            athlete_event_results["medal"],
            athlete_bio["sex"],
            athlete_bio["country_noc"],
        )
        .agg(
            avg(athlete_bio["weight"]).alias("avg_weight"),
            avg(athlete_bio["height"]).alias("avg_height"),
        )
        .withColumn("timestamp", current_timestamp())
    )

    result_df.show()

    gold_table_path = os.path.join(GOLD_PATH, "avg_stats")
    result_df.write.mode("overwrite").parquet(gold_table_path)
    print(f"Saved Gold table to {gold_table_path}")

    spark.stop()


if __name__ == "__main__":
    main()
