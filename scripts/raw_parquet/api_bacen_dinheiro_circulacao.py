import pyspark
from pyspark import SparkContext 
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').getOrCreate()

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

# Pegando todas as informacoes com varias requisiçoes
tabela_final = pd.DataFrame()
pular_indice = 0

while True:
    link = f"https://olinda.bcb.gov.br/olinda/servico/mecir_dinheiro_em_circulacao/versao/v1/odata/informacoes_diarias?$top=10000&$skip={pular_indice}&$orderby=Data%20desc&$format=json"
    requisicao = requests.get(link)
    informacoes = requisicao.json()
    tabela = pd.DataFrame(informacoes["value"])
    if len(informacoes["value"]) < 1:
        break
    tabela_final = pd.concat([tabela_final, tabela])
    pular_indice += 10000

# Criando Dataframe spark a partir de um Dataframe pandas
df = spark.createDataFrame(tabela_final)

# Criando uma coluna nova com o formato de data
df = df.withColumn("dt", current_date())

# Convertendo coluna dt para string (servirá como particionamento para a ingestão)
df = df.withColumn("dt", col("dt").cast(StringType()))

# Função para ingerir os dados na camada raw
def carga_parquet(path, coluna_particao, sink):
    df.write.mode("append")\
    .format("parquet")\
    .partitionBy(coluna_particao)\
    .save(f"s3a://datalake-dados/{sink}/{path}/")

# Definindo variávies para a ingestão   
coluna_particao = "dt"
sink = "raw" 
path = "bacen_dinheiro_circulacao"

# Executando a função
carga_parquet(path, coluna_particao, sink)
print("Carga dos dados finalizada.")

# Finalizando a sessão spark
spark.stop()