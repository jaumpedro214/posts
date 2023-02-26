from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

MASTER_URI = "spark://spark:7077"

if __name__ == "__main__":
    # spark-submit --packages io.delta:delta-core_2.12:2.1.0,com.google.cloud.spark:spark-3.1-bigquery:0.28.0-preview aggregate_delta_gcs_to_gbq_table.py

    builder = SparkSession.builder\
        .master(MASTER_URI)\
        .appName("Aggregate Delta Table into GBQ table")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Configure GCS temporary bucket
    bucket = "censo-ensino-superior"
    spark.conf.set('temporaryGcsBucket', bucket)

    # Reduce Spark partitions
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    # Reduce log level
    spark.sparkContext.setLogLevel("ERROR")

    df_censo = (
        spark.read
            .format("delta")
            .load("gs://censo-ensino-superior/cens_cursos")
            .limit(10)
    )

    # The dataset must exist in BigQuery
    df_censo.write\
        .format("bigquery")\
        .mode("overwrite")\
        .option("dataset", "censo_ensino_superior")\
        .option("table", "censo_ensino_superior.censo_cursos")\
        .option("createDisposition", "CREATE_IF_NEEDED")\
        .save()

    df_censo.show(10)