# leitura_apis_spark_airflow
Repositório contendo projeto pessoal de estudos utilizando o Apache Spark para o ETL e o Airflow para orquestração

O ambiente foi construído usando o docker, com os containeres do Airflow e Spark.

O objetivo foi consumir api's públicas/gratuitas e realizar a ingestão em uma camada raw (dado cru). 
Na camada trusted, foi realizada a limpeza dos dados, trazendo a última data de processamento de cada registro. 
E por fim, na camada refined, foi realizada a ingestão com os dados tratados e prontos para consumo.

A orquestração foi definida usando o Airflow, usando o SparkSubmitOperator para realizar a conexão com o container do spark.

Foram consumidas api's de ações e api's com dados públicos, como dados da covid. 

Todo o projeto foi executado em uma máquina Ubuntu na AWS, usando o Lambda para programar o start-stop da VM.
