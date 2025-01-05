from pyspark.sql.functions import from_json, col
from get_athlete_results import athlete_event_results_schema


def parse_results(df):
    json_df = df.select(
        from_json(col("value").cast("string"), athlete_event_results_schema).alias(
            "data"
        )
    ).select("data.*")

    return json_df
