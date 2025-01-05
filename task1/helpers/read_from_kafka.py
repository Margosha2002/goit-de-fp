from config import kafka, my_name


def read_from_kafka(spark, topic):
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka["bootstrap_servers"][0])
        .option("kafka.security.protocol", kafka["security_protocol"])
        .option("kafka.sasl.mechanism", kafka["sasl_mechanism"])
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka["username"]}" password="{kafka["password"]}";',
        )
        .option("subscribe", f"{my_name}_{topic}")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5")
        .load()
    )

    return df
