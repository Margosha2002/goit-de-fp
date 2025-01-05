from config import kafka, my_name


def write_stream_to_kafka(topic):
    def write(batch_df, _):
        batch_df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option(
            "kafka.bootstrap.servers", kafka["bootstrap_servers"][0]
        ).option("kafka.security.protocol", kafka["security_protocol"]).option(
            "kafka.sasl.mechanism", kafka["sasl_mechanism"]
        ).option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka["username"]}" password="{kafka["password"]}";',
        ).option(
            "topic", f"{my_name}_{topic}"
        ).save()

    return write


def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option(
        "kafka.bootstrap.servers", kafka["bootstrap_servers"][0]
    ).option("kafka.security.protocol", kafka["security_protocol"]).option(
        "kafka.sasl.mechanism", kafka["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka["username"]}" password="{kafka["password"]}";',
    ).option(
        "topic", f"{my_name}_{topic}"
    ).save()
