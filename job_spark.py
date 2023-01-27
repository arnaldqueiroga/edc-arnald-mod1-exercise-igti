from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import SparkSession

# Definicao da Spark Session
spark = (SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
)

# Leitura de dados
enem = (
    spark
    .read
    .format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", ";")
    .load("s3://datalake-arnald-900605953217/raw-data/")
)

# Escreve a tabela em staging em formato parquet
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year")
    .save("s3://datalake-arnald-900605953217/staging/enem")
)