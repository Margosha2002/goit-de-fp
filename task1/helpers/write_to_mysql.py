from helpers.config import mysql, my_name


def write_stream_to_mysql(table_name):
    def write(batch_df, _):
        batch_df.write.format("jdbc").options(
            url=mysql["url"],
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=f"{my_name}_{table_name}",
            user=mysql["user"],
            password=mysql["password"],
        ).mode("append").save()

    return write
