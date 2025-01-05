from pyspark.sql.types import (
    StructType,
    StringType,
    DoubleType,
    IntegerType,
)
from pyspark.sql.functions import col
from config import mysql


athlete_bio_schema = (
    StructType()
    .add("athlete_id", IntegerType())
    .add("name", StringType())
    .add("sex", StringType())
    .add("born", StringType())
    .add("height", DoubleType())
    .add("weight", DoubleType())
    .add("country", StringType())
    .add("country_noc", StringType())
    .add("description", StringType())
    .add("special_notes", StringType())
)


# Етап 1: Зчитати дані
def get_athlete_bio(spark):
    athlete_bio_df = (
        spark.read.format("jdbc")
        .options(
            url=mysql["url"],
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="athlete_bio",
            user=mysql["user"],
            password=mysql["password"],
        )
        .load()
    )
    # Етап 2: Фільтрація
    filtered_bio_df = athlete_bio_df.filter(
        (col("height").cast("float").isNotNull())
        & (col("weight").cast("float").isNotNull())
    )

    return filtered_bio_df
