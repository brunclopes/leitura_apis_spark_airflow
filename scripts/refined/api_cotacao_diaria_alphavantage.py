import pyspark
from delta import configure_spark_with_delta_pip
from pyspark import SparkContext 

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Bibliotecas Pyspark
from pyspark.sql.functions import current_date
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

# Criação de variáveis  
path = "cotacao_diaria_alphavantage"
source = "trusted"
sink = "refined"

# Leitura das cotações diárias da empresa Amazon
df_amazon = spark.read.format("delta").load(f"s3a://datalake-bruno/{source}/{path}_amazon/")

# Leitura das cotações diárias da empresa Ambev
df_ambev = spark.read.format("delta").load(f"s3a://datalake-bruno/{source}/{path}_ambev/")

# Leitura das cotações diárias da empresa Bradesco
df_bradesco = spark.read.format("delta").load(f"s3a://datalake-bruno/{source}/{path}_bradesco/")

# Leitura das cotações diárias da empresa Itau
df_itau = spark.read.format("delta").load(f"s3a://datalake-bruno/{source}/{path}_itau/")

# Leitura das cotações diárias da empresa Microsoft
df_microsoft = spark.read.format("delta").load(f"s3a://datalake-bruno/{source}/{path}_microsoft/")


# Union entre os dataframes amazon e ambev
df_union1 = df_amazon.unionByName(df_ambev)

# Union entre o dataframe anterior e o dataframe bradesco
df_union2 = df_union1.unionByName(df_bradesco)

# Union entre o dataframe anterior e o dataframe itau
df_union3 = df_union2.unionByName(df_itau)

# Union entre o dataframe anterior e o dataframe microsoft
df_union_final = df_union3.unionByName(df_microsoft)

# Mudando o dataframe
df = df_union_final

# Criando coluna dt_ingestion para o particionamento
df = df.withColumn(f"dt_ingestion_{sink}", current_date().cast(StringType()))

# Função para ingerir os dados na camada curated
def carga_delta(path, coluna_particao, sink):
    df.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "True")\
    .partitionBy(coluna_particao)\
    .save(f"s3a://datalake-bruno/{sink}/{path}/")

# Variáveis para a ingestão
coluna_particao = f"dt_ingestion_{sink}"
    
# Executando a função
carga_delta(path, coluna_particao, sink)
print("Carga dos dados finalizada.")

# Finalizando a sessão spark
spark.stop()
