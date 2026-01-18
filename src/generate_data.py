import os
from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.spark_session import get_spark

def main():
    spark = get_spark("generate-data")

    out = os.path.abspath("data")
    os.makedirs(out, exist_ok=True)

    # Ajusta estos números según tiempo/espacio (empezamos con 5M events y luego subimos)
    n_users = 500_000
    n_orders = 2_000_000
    n_events = 10_000_000
    n_ads = 2_000_000

    # Users
    users = (
        spark.range(0, n_users).withColumnRenamed("id", "user_id")
        .withColumn("country", F.expr("element_at(array('AT','DE','IT','ES','FR','NL'), cast(rand()*6 as int)+1)"))
        .withColumn("signup_ts", F.expr("timestampadd(DAY, -cast(rand()*365 as int), current_timestamp())"))
    )

    # Orders
    orders = (
        spark.range(0, n_orders).withColumnRenamed("id", "order_id")
        .withColumn("user_id", (F.rand()*n_users).cast("long"))
        .withColumn("order_ts", F.expr("timestampadd(MINUTE, -cast(rand()*60*24*180 as int), current_timestamp())"))
        .withColumn("product_type", F.expr("element_at(array('photobook','prints','calendar','cards'), cast(rand()*4 as int)+1)"))
        .withColumn("price_eur", (F.rand()*50 + 5).cast("double"))
        .withColumn("quantity", (F.rand()*3 + 1).cast("int"))
        .withColumn("revenue_eur", F.col("price_eur") * F.col("quantity"))
    )

    # Events (app usage)
    events = (
        spark.range(0, n_events).withColumnRenamed("id", "event_id")
        .withColumn("user_id", (F.rand()*n_users).cast("long"))
        .withColumn("event_ts", F.expr("timestampadd(SECOND, -cast(rand()*60*60*24*30 as int), current_timestamp())"))
        .withColumn("event_name", F.expr("element_at(array('open_app','view_product','add_to_cart','checkout_start','purchase'), cast(rand()*5 as int)+1)"))
        .withColumn("device", F.expr("element_at(array('ios','android','web'), cast(rand()*3 as int)+1)"))
        .withColumn("session_id", F.sha2(F.concat_ws("-", F.col("user_id"), (F.rand()*10_000_000).cast("long")), 256))
    )

    # Ads (marketing spend + clicks)
    ads = (
        spark.range(0, n_ads).withColumnRenamed("id", "ad_event_id")
        .withColumn("campaign_id", (F.rand()*50_000).cast("long"))
        .withColumn("channel", F.expr("element_at(array('google_ads','meta_ads','influencer','tiktok'), cast(rand()*4 as int)+1)"))
        .withColumn("ad_ts", F.expr("timestampadd(MINUTE, -cast(rand()*60*24*60 as int), current_timestamp())"))
        .withColumn("spend_eur", (F.rand()*5).cast("double"))
        .withColumn("clicks", (F.rand()*3).cast("int"))
        .withColumn("impressions", (F.rand()*20 + 1).cast("int"))
    )

    # Introducimos algunos "errores" para que DQ detecte cosas (duplicados, nulls, fechas futuras)
    users_bad = users.withColumn("country", F.when(F.rand() < 0.001, F.lit(None)).otherwise(F.col("country")))
    orders_bad = orders.withColumn("revenue_eur", F.when(F.rand() < 0.001, F.lit(-1.0)).otherwise(F.col("revenue_eur")))
    events_bad = events.withColumn("event_ts", F.when(F.rand() < 0.001, F.expr("timestampadd(DAY, 5, current_timestamp())")).otherwise(F.col("event_ts")))

    users_bad.write.mode("overwrite").parquet(os.path.join(out, "users"))
    orders_bad.write.mode("overwrite").parquet(os.path.join(out, "orders"))
    events_bad.write.mode("overwrite").parquet(os.path.join(out, "events"))
    ads.write.mode("overwrite").parquet(os.path.join(out, "ads"))

    print("✅ Data generated in ./data (parquet)")
    spark.stop()

if __name__ == "__main__":
    main()

