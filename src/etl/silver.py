import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.spark_session import get_spark


def dq_metrics(df, name: str, id_col: str = None) -> dict:
    total = df.count()

    # null rates
    nulls = {}
    for c in df.columns:
        nulls[c] = df.filter(F.col(c).isNull()).count()

    null_rate_max = float(max(nulls.values()) / total) if total else 0.0

    # duplicate rate on id_col (if exists)
    dup_rate = None
    if id_col and id_col in df.columns:
        distinct_ids = df.select(id_col).distinct().count()
        dup_rate = 0.0 if total == 0 else float(1 - distinct_ids / total)

    return {
        "table": name,
        "rows": int(total),
        "null_rate_max": float(null_rate_max),
        "dup_rate_id": float(dup_rate) if dup_rate is not None else -1.0,
    }


def main():
    spark = get_spark("etl-silver")

    bronze_dir = os.path.abspath("warehouse/bronze")
    silver_dir = os.path.abspath("warehouse/silver")
    os.makedirs(silver_dir, exist_ok=True)

    users = spark.read.parquet(os.path.join(bronze_dir, "users"))
    orders = spark.read.parquet(os.path.join(bronze_dir, "orders"))
    events = spark.read.parquet(os.path.join(bronze_dir, "events"))
    ads = spark.read.parquet(os.path.join(bronze_dir, "ads"))

    # --- USERS: dedupe by user_id keeping latest ingest_ts
    if "user_id" in users.columns:
        w = Window.partitionBy("user_id").orderBy(F.col("ingest_ts").desc())
        users = users.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # --- ORDERS: cast amounts, parse dates if present
    for c in ["amount", "total_amount", "price"]:
        if c in orders.columns:
            orders = orders.withColumn(c, F.col(c).cast("double"))

    for c in ["created_at", "order_ts", "timestamp"]:
        if c in orders.columns:
            orders = orders.withColumn(c, F.to_timestamp(F.col(c)))

    # --- EVENTS: parse timestamp + keep only valid event_name if exists
    for c in ["event_ts", "timestamp", "created_at"]:
        if c in events.columns:
            events = events.withColumn(c, F.to_timestamp(F.col(c)))

    if "event_name" in events.columns:
        events = events.filter(F.col("event_name").isNotNull())

    # --- ADS: cast numeric metrics
    for c in ["spend", "clicks", "impressions", "conversions"]:
        if c in ads.columns:
            ads = ads.withColumn(c, F.col(c).cast("double"))

    # Write silver
    users.write.mode("overwrite").parquet(os.path.join(silver_dir, "users"))
    orders.write.mode("overwrite").parquet(os.path.join(silver_dir, "orders"))
    events.write.mode("overwrite").parquet(os.path.join(silver_dir, "events"))
    ads.write.mode("overwrite").parquet(os.path.join(silver_dir, "ads"))

    # Data Quality report (simple, CV-friendly)
    report = []
    report.append(dq_metrics(users, "users", "user_id"))
    report.append(dq_metrics(orders, "orders", "order_id" if "order_id" in orders.columns else None))
    report.append(dq_metrics(events, "events", "event_id" if "event_id" in events.columns else None))
    report.append(dq_metrics(ads, "ads", "ad_id" if "ad_id" in ads.columns else None))

    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

    schema = StructType([
        StructField("table", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("null_rate_max", DoubleType(), False),
        StructField("dup_rate_id", DoubleType(), False),
    ])

    report_df = spark.createDataFrame(report, schema=schema)
    report_df.coalesce(1).write.mode("overwrite").json(os.path.join(silver_dir, "dq_report"))

    print("✅ Silver written to ./warehouse/silver")
    print("✅ DQ report written to ./warehouse/silver/dq_report")
    spark.stop()


if __name__ == "__main__":
    main()

