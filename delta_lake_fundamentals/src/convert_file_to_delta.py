from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestWriteDeltaLake")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

# Reduce the number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 4)

FILENAME = "/data/acidentes/datatran2020.csv"

df = spark.read.csv(FILENAME, header=True, sep=";", inferSchema=True)

df.show(10)

df.write.format("delta").mode("overwrite").save("/data/delta/acidentes")
