from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestWriteDeltaLake")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

df = spark.range(100)

df.write.format("delta").save("/data/delta-table")

# run command 
# spark-submit --packages io.delta:delta-core_2.12:2.1.0 write_file.py