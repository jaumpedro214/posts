from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
            # .limit(1000)
    )

    df_censo.printSchema()
    # exit()

    # df_censo.show(10)

    df_censo = (
        df_censo
        # Bachelor and Licenciatura TP_GRAU_ACADEMICO = 4
        .filter( 
            (F.col('TP_GRAU_ACADEMICO') == "1")  
            | (F.col('TP_GRAU_ACADEMICO') == "4")
        )
        # Group by NO_CINE_AREA_DETALHADA, CO_CINE_AREA_DETALHADA
        .groupBy(
            'CO_CINE_AREA_DETALHADA', 'CO_UF', 'NU_ANO_CENSO'
        )
        .agg(
            F.max('NO_CINE_AREA_DETALHADA').alias('NO_CINE_AREA_DETALHADA'),

            F.max('NO_CINE_AREA_ESPECIFICA').alias('NO_CINE_AREA_ESPECIFICA'),
            F.max('CO_CINE_AREA_ESPECIFICA').alias('CO_CINE_AREA_ESPECIFICA'),
            F.max('NO_CINE_AREA_GERAL').alias('NO_CINE_AREA_GERAL'),
            F.max('CO_CINE_AREA_GERAL').alias('CO_CINE_AREA_GERAL'),

            F.max('SG_UF').alias('SG_UF'),
            F.max('NO_REGIAO').alias('NO_REGIAO'),
            F.max('CO_REGIAO').alias('CO_REGIAO'),

            F.count('CO_CURSO').alias('QT_CO_CURSO'),
            F.sum('QT_CURSO').alias('QT_CURSO'),
            F.sum('QT_VG_TOTAL').alias('QT_VG_TOTAL'),
            F.sum('QT_ING').alias('QT_ING'),

            F.sum('QT_ING_0_17').alias('QT_ING_0_17'),
            F.sum('QT_ING_18_24').alias('QT_ING_18_24'),
            F.sum('QT_ING_25_29').alias('QT_ING_25_29'),
            F.sum('QT_ING_30_34').alias('QT_ING_30_34'),
            F.sum('QT_ING_35_39').alias('QT_ING_35_39'),
            F.sum('QT_ING_40_49').alias('QT_ING_40_49'),
            F.sum('QT_ING_50_59').alias('QT_ING_50_59'),

            F.sum('QT_ING_60_MAIS').alias('QT_ING_60_MAIS'),
            F.sum('QT_ING_BRANCA').alias('QT_ING_BRANCA'),
            F.sum('QT_ING_PRETA').alias('QT_ING_PRETA'),
            F.sum('QT_ING_PARDA').alias('QT_ING_PARDA'),
            F.sum('QT_ING_AMARELA').alias('QT_ING_AMARELA'),
            F.sum('QT_ING_INDIGENA').alias('QT_ING_INDIGENA'),
            F.sum('QT_ING_CORND').alias('QT_ING_CORND'),

            F.sum('QT_ING_FEM').alias('QT_ING_FEM'),
            F.sum('QT_ING_MASC').alias('QT_ING_MASC'),
        )
    )

    # print( df_censo.count() )
    # exit()
    
    # The dataset must exist in BigQuery
    df_censo.write\
        .format("bigquery")\
        .mode("overwrite")\
        .option("temporaryGcsBucket", bucket)\
        .option("dataset", "censo_ensino_superior")\
        .option("table", "censo_ensino_superior.cursos_graduacao_e_licenciatura")\
        .option("createDisposition", "CREATE_IF_NEEDED")\
        .save()
    