import os
from pyspark.sql import functions as F
from src.spark_session import get_spark


def main():
    spark = get_spark("etl-bronze")

    data_dir = os.path.abspath("data")
    out_dir = os.path.abspath("warehouse/bronze")
    os.makedirs(out_dir, exist_ok=True)

    users = spark.read.parquet(os.path.join(data_dir, "users"))
    users = users.withColumn("ingest_ts", F.current_timestamp())

    orders = spark.read.parquet(os.path.join(data_dir, "orders"))
    orders = orders.withColumn("ingest_ts", F.current_timestamp())

    events = spark.read.parquet(os.path.join(data_dir, "events"))
    events = events.withColumn("ingest_ts", F.current_timestamp())

    ads = spark.read.parquet(os.path.join(data_dir, "ads"))
    ads = ads.withColumn("ingest_ts", F.current_timestamp())

    users.write.mode("overwrite").parquet(os.path.join(out_dir, "users"))
    orders.write.mode("overwrite").parquet(os.path.join(out_dir, "orders"))
    events.write.mode("overwrite").parquet(os.path.join(out_dir, "events"))
    ads.write.mode("overwrite").parquet(os.path.join(out_dir, "ads"))

    print("✅ Bronze written to ./warehouse/bronze")
    spark.stop()


if __name__ == "__main__":
    main()

