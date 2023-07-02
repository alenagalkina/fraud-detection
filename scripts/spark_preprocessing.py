import findspark
import numpy as np
import pandas as pd

findspark.init()
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, hour, minute, second
from pyspark.sql.types import DoubleType, IntegerType, TimestampType


def main():
    conf = SparkConf().setAppName("Spark App")
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    sc = SparkContext(conf=conf)

    sql = SQLContext(sc)
    path = "2022-10-05.txt"
    data = sql.read.text(path)
    print("count:", data.count())

    header = data.first()[0]
    schema = header.split(" | ")
    df_new = (
        data.filter(data["value"] != header)
        .rdd.map(lambda x: x[0].split(","))
        .toDF(schema)
    )
    df_new = (
        df_new.withColumn("tx_datetime", df_new.tx_datetime.cast(TimestampType()))
        .withColumn("customer_id", df_new.customer_id.cast(IntegerType()))
        .withColumn("terminal_id", df_new.terminal_id.cast(IntegerType()))
        .withColumn("tx_amount", df_new.tx_amount.cast(DoubleType()))
        .withColumn("tx_time_seconds", df_new.tx_time_seconds.cast(IntegerType()))
        .withColumn("tx_time_days", df_new.tx_time_days.cast(IntegerType()))
        .withColumn("tx_fraud", df_new.tx_fraud.cast(IntegerType()))
        .withColumn("tx_fraud_scenario", df_new.tx_fraud_scenario.cast(IntegerType()))
    )

    df_new = (
        df_new.withColumn("hour", hour(col("tx_datetime")))
        .withColumn("minute", minute(col("tx_datetime")))
        .withColumn("second", second(col("tx_datetime")))
    )

    df_new = df_new.dropDuplicates(["# tranaction_id"])
    df_new = df_new.filter(
        (df_new["customer_id"] != -999999)
        & (df_new["customer_id"] != 999999)
        & (df_new["tx_amount"] != 0)
    )
    print("final_shape:", df_new.count())
    defaultFS = sc._jsc.hadoopConfiguration().get("fs.defaultFS")
    data.repartition(1).write.parquet(
        defaultFS + "/tmp/preprocessed/" + path[:-4] + ".parquet"
    )


if __name__ == "__main__":
    main()
