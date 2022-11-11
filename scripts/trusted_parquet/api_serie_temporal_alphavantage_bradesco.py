import pyspark
from pyspark import SparkContext 
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').getOrCreate()

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
empresa = "bradesco"
path = f"serie_temporal_alphavantage_{empresa}"
source = "raw"
sink = "trusted"

# Leitura da base
df = spark.read.format("parquet").load(f"s3a://datalake-dados/{source}/{path}/")

# Criação de tempview
df.createOrReplaceTempView("df")

# Criação de dataframe com a maior data de ingestão para o join
df_data = spark.sql("""
SELECT 
timestamp as start,
max(date(dt)) as date
from df
group by start
""")

# Join com a data maxima de ingestão para remover os dados duplicados
cond_join = [df.timestamp == df_data.start, df.dt == df_data.date]
df_join = df.join(df_data, on=cond_join, how="inner").drop("date", "start")

df = df_join

# Renomeando coluna dt para coluna dt_raw
df = df.withColumnRenamed("dt", f"dt_ingestion_{source}")

# Criando coluna dt_ingestion para o particionamento
df = df.withColumn(f"dt_ingestion_{sink}", current_date().cast(StringType()))

# Função para ingerir os dados na camada trusted
def carga_parquet(path, coluna_particao, sink):
    df.write.mode("overwrite")\
    .format("parquet")\
    .partitionBy(coluna_particao)\
    .save(f"s3a://datalake-dados/{sink}/{path}/")

# Definindo variávies para a ingestão 
coluna_particao = f"dt_ingestion_{sink}"
    
# Executando a função
carga_parquet(path, coluna_particao, sink)
print("Carga dos dados finalizada.")

# Finalizando a sessão spark
spark.stop()