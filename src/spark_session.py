from pyspark.sql import SparkSession


def get_spark(app_name: str = "journi-analytics-warehouse") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[3]")  # baja paralelismo para no reventar memoria
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # reduce presión de diccionario parquet (suele ser el culpable en strings)
        .config("parquet.enable.dictionary", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

