from pyspark.sql.functions import avg, current_timestamp


def aggregate_athletes(results_df, bio_df):
    # Етап 4: Об’єднати дані
    joined_df = results_df.join(
        bio_df, results_df["athlete_id"] == bio_df["athlete_id"], "left"
    )
    # Етап 5: Cередній зріст і вага
    aggregated_df = (
        joined_df.groupBy(
            results_df["sport"],
            results_df["medal"],
            bio_df["sex"],
            bio_df["country_noc"],
        )
        .agg(
            avg(bio_df["height"]).alias("avg_height"),
            avg(bio_df["weight"]).alias("avg_weight"),
        )
        .withColumn("timestamp", current_timestamp())
    )

    return aggregated_df
