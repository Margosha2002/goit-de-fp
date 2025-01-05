from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
)
from config import mysql


athlete_event_results_schema = (
    StructType()
    .add("edition", StringType())
    .add("edition_id", IntegerType())
    .add("country_noc", StringType())
    .add("sport", StringType())
    .add("event", StringType())
    .add("result_id", IntegerType())
    .add("athlete", StringType())
    .add("athlete_id", IntegerType())
    .add("pos", StringType())
    .add("medal", StringType())
)


def get_athlete_results(spark):
    athlete_event_results_df = (
        spark.read.format("jdbc")
        .options(
            url=mysql["url"],
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="athlete_event_results",
            user=mysql["user"],
            password=mysql["password"],
        )
        .load()
    )

    athlete_event_results_df.show()

    return athlete_event_results_df
