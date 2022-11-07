import pyspark
from delta import configure_spark_with_delta_pip
from pyspark import SparkContext 

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Bibliotecas Python
import requests
import pandas as pd

# Bibliotecas Pyspark
from pyspark.sql.functions import col, current_date
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

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

# Função para ler os dados da api e retornar em json
def capturar_dados(url, parametros):
    res = requests.get(url, params=parametros).json()
    res = res["dados"]
    return res

# Definindo a url e executando a função
url = "https://dadosabertos.camara.leg.br/api/v2/deputados"
parametros = {}

dados = capturar_dados(url, parametros)


# Transformando o json em um dataframe Pandas
df = pd.DataFrame(dados)

# Definindo o schema para conversão do dataframe Spark 
schema = StructType([
    StructField("id", IntegerType(), True)
    ,StructField("uri", StringType(), True)
    ,StructField("nome", StringType(), True)
    ,StructField("siglaPartido", StringType(), True)
    ,StructField("uriPartido", StringType(), True)
    ,StructField("siglaUf", StringType(), True)
    ,StructField("idLegislatura", IntegerType(), True)
    ,StructField("urlFoto", StringType(), True)
    ,StructField("email", StringType(), True)
])

# Criando dataframe Spark
df = spark.createDataFrame(dados, schema = schema)


# Selecionando colunas
df = df.select(
    df.id
    ,df.nome
    ,df.siglaPartido
    ,df.siglaUf
    ,df.idLegislatura
    ,df.urlFoto
)

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
path = "info_deputados"

# Executando a função
carga_delta(path, coluna_particao, sink)
print("Carga dos dados finalizada.")

# Finalizando a sessão spark
spark.stop()