from awsglue.transforms import *
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


dyf = glueContext.create_dynamic_frame.from_catalog(
    database="enem_pdf_project", table_name="content"
)
dyf.printSchema()
df = dyf.toDF()


df = df.withColumn(
    "year", F.regexp_extract(F.col("original_uri"), ".+pdf_([0-9]{4})", 1)
)

df = (
    df.withColumn("text", F.lower(F.col("content")))
    .withColumn(
        "text",
        F.regexp_replace(
            F.col("text"), "(questão [0-9]+)", "<QUESTION_START_MARKER>$1"
        ),
    )
    .withColumn("text", F.split(F.col("text"), "<QUESTION_START_MARKER>"))
    .withColumn("question", F.explode(F.col("text")))
    .withColumn(
        "question_number", F.regexp_extract(F.col("question"), "questão ([0-9]+)", 1)
    )
    .drop("content", "text")
)

df.write.csv("s3://enem-bucket/processed/", mode="overwrite", header=True)

job.commit()
