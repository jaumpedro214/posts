from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, BooleanType

from delta import configure_spark_with_delta_pip

MASTER_URI = "spark://spark:7077"


if __name__ == "__main__":
    # spark-submit --packages io.delta:delta-core_2.12:2.1.0 --master spark://spark:7077 insert_csv_into_delta_table_gcs.py

    builder = SparkSession.builder\
        .master(MASTER_URI)\
        .appName("Insert CSV Censo into Delta Table")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Read the CSV file
    df_cursos = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("encoding", "ISO-8859-1")
        .option("inferSchema", "true")
        .load("/data/MICRODADOS_CADASTRO_CURSOS_*.CSV")
    )

    # Select Columns
    df_cursos = df_cursos.select(
        [   
            # Year
            "NU_ANO_CENSO",

            # Course AND Institution
            "CO_IES",
            "NO_CURSO",
            "CO_CURSO",

            "QT_CURSO",
            "QT_VG_TOTAL",

            # Total of ingressants
            "QT_ING",

            # Age
            "QT_ING_0_17",
            "QT_ING_18_24",
            "QT_ING_25_29",
            "QT_ING_30_34",
            "QT_ING_35_39",
            "QT_ING_40_49",
            "QT_ING_50_59",
            "QT_ING_60_MAIS",

            # Skin Color
            "QT_ING_BRANCA",
            "QT_ING_PRETA",
            "QT_ING_PARDA",
            "QT_ING_AMARELA",
            "QT_ING_INDIGENA",
            "QT_ING_CORND",

            # Gender
            "QT_ING_FEM",
            "QT_ING_MASC",

            # Place
            "NO_REGIAO",
            "CO_REGIAO",
            "NO_UF",
            "SG_UF",
            "CO_UF",
            "NO_MUNICIPIO",
            "CO_MUNICIPIO",
            "IN_CAPITAL",

            "TP_DIMENSAO",
            "TP_ORGANIZACAO_ACADEMICA",
            "TP_CATEGORIA_ADMINISTRATIVA",
            "TP_REDE",

            # Area of Knowledge (CINE)
            "NO_CINE_ROTULO",
            "CO_CINE_ROTULO",
            "CO_CINE_AREA_GERAL",
            "NO_CINE_AREA_GERAL",
            "CO_CINE_AREA_ESPECIFICA",
            "NO_CINE_AREA_ESPECIFICA",
            "CO_CINE_AREA_DETALHADA",
            "NO_CINE_AREA_DETALHADA",


            "TP_GRAU_ACADEMICO",
            "IN_GRATUITO",
            "TP_MODALIDADE_ENSINO",
            "TP_NIVEL_ACADEMICO",
        ]
    )

    # cast columns
    for col in df_cursos.columns:
        if col in ["NU_ANO_CENSO"] or col.startswith("QT_"):
            df_cursos = df_cursos.withColumn(col, df_cursos[col].cast(IntegerType()))
        elif col.startswith("IN_"):
            df_cursos = df_cursos.withColumn(col, df_cursos[col].cast(BooleanType()))
        else:
            df_cursos = df_cursos.withColumn(col, df_cursos[col].cast(StringType()))

    # df_cursos.show(10)

    df_cursos.write\
        .format("delta")\
        .partitionBy(["NU_ANO_CENSO"])\
        .mode("overwrite")\
        .save("gs://censo-ensino-superior/cens_cursos")
