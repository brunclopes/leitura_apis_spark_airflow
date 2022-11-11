import pyspark
from pyspark import SparkContext 
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').getOrCreate()

import requests
import pandas as pd
from io import StringIO
import time

# Bibliotecas Pyspark
from pyspark.sql.functions import col, current_date
from pyspark.sql.types import StringType

from chaves import access_key, secret_key, endpoint, key_alphavantage

# Função para conexão do spark com o Data lake s3
def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"http://{endpoint}")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.SSL.enabled", "false")
    
load_config(spark.sparkContext)

# Lendo API que informa as ações de uma determinada empresa
empresa = "ambev"
url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={empresa}&apikey={key_alphavantage}&datatype=csv"
r = requests.get(url)

# Criando dataframe Pandas a partir do texto vindo da api
tabela = pd.read_csv(StringIO(r.text))

# Criação de dataframe spark
df_acoes = spark.createDataFrame(tabela)

# Selecionando colunas
df_acoes = df_acoes.select(
             col("symbol").alias("acao")
            ,col("name")
            ,col("region")
            ,col("currency")
            ,col("marketOpen")
            ,col("marketClose")
)

# Trazendo as ações e transformando em uma lista
acoes = df_acoes.select(col("acao")).distinct().rdd.flatMap(lambda x: x).collect()
acoes = acoes[0:5]

# Parando a execução por 1 min (a API possui 5 requisições por minuto)
time.sleep(60)

# Lendo API que traz a última cotação do dia com base nas ações filtradas. É criado um dataframe vazio e criado
# um loop que concatena os dataframes com as informações de cada ação
compilada = pd.DataFrame()

for acao in acoes:
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={acao}&apikey={key_alphavantage}&datatype=csv"
    r = requests.get(url)
    tabela = pd.read_csv(StringIO(r.text))
    tabela["symbol"] = acao
    lista_tabelas = [compilada, tabela]
    compilada = pd.concat(lista_tabelas)
    
# Criando dataframe spark   
df = spark.createDataFrame(compilada)

# Join entre o dataframe de informações das ações e o dataframe das cotações 
cond_join = [df.symbol == df_acoes.acao]
df_joinado = df.join(df_acoes, on = cond_join).drop("acao")

df_joinado = df_joinado.select(
                col("timestamp")
                ,col("symbol")
                ,col("name")
                ,col("region")
                ,col("currency")
                ,col("marketOpen")
                ,col("marketClose")
                ,col("open")
                ,col("low")
                ,col("high")
                ,col("close")
                ,col("volume")
)

df = df_joinado

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
path = f"serie_temporal_alphavantage_{empresa}"

# Executando a função
carga_parquet(path, coluna_particao, sink)
print("Carga dos dados finalizada.")

# Finalizando a sessão spark
spark.stop()