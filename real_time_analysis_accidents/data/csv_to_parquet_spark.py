# import spark session
from pyspark.sql import SparkSession
from collections import defaultdict

# create spark session
spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()

COLUMNS = [ 'id', 'pesid', 'data_inversa',
            'dia_semana', 'horario', 'uf', 'br', 'km',
            'municipio', 'causa_acidente', 'tipo_acidente',
            'classificacao_acidente', 'fase_dia',
            'sentido_via', 'condicao_metereologica',
            'tipo_pista', 'tracado_via', 'uso_solo',
            'id_veiculo', 'tipo_veiculo', 'marca', 
            'tipo_envolvido', 'estado_fisico', 'idade','sexo',
          ]
CONFIG = defaultdict(lambda: {"delimiter":";", "encoding":"ISO-8859-1"})
CONFIG[2015] = {"delimiter":",", "encoding":"ISO-8859-1"}
CONFIG[2020] = {"delimiter":";", "encoding":"UTF-8"}

YEARS = range(2015, 2021)
FILES = [ "data/acidentes{year}.csv".format(year=year) for year in YEARS ]

# Read all CSV files in this folder
# and transform them into a parquet file
for year, file in zip(YEARS, FILES):
    df = spark.\
        read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .option("delimiter", CONFIG[year]["delimiter"])\
        .option("encoding", CONFIG[year]["encoding"])\
        .csv(file)\
        .write.parquet(
            "accidents.parquet", 
            mode="append"
        )

