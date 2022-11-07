import pyspark
from delta import configure_spark_with_delta_pip
from pyspark import SparkContext 

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

import requests
import pandas as pd

# Bibliotecas Pyspark
from pyspark.sql.functions import col, current_date
from pyspark.sql.types import StringType

from chaves import access_key, secret_key, endpoint

# Função para conexão do spark com o Data lake s3
def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"http://{endpoint}")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.SSL.enabled", "false")
    
load_config(spark.sparkContext)

# Leitura da api

anos = ["2020", "2021", "2022", "2023", "2024", "2025", "2026", "2027", "2028",
"2029", "2030", "2031", "2032", "2033", "2034", "2035"]

tabela_final = pd.DataFrame()

for ano in anos:
    r = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{ano}").json()
    tabela = pd.DataFrame(r)
    tabela_final = pd.concat([tabela_final, tabela])

# Criação de dataframe spark
df = spark.createDataFrame(tabela_final)


# Criando uma coluna nova com o formato de data
df = df.withColumn("dt", current_date())

# Convertendo coluna dt para string (servirá como particionamento para a ingestão)
df = df.withColumn("dt", col("dt").cast(StringType()))

# Função para ingerir os dados na camada curated
def carga_delta(path, coluna_particao, sink):
    df.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "True")\
    .partitionBy(coluna_particao)\
    .save(f"s3a://datalake-bruno/{sink}/{path}/")

# Definindo variávies para a ingestão
coluna_particao = "dt"
sink = "raw" 
path = "info_feriados"

# Executando a função
carga_delta(path, coluna_particao, sink)
print("Carga dos dados finalizada.")

# Finalizando a sessão spark
spark.stop()