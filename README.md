# leitura_apis_spark_airflow
Repositório contendo projeto pessoal de estudos utilizando o Apache Spark para o ETL e o Airflow para orquestração

O ambiente foi construído usando o docker, com os containeres do Airflow e Spark.

O objetivo foi consumir api's públicas/gratuitas e realizar a ingestão em uma camada raw (dado cru), em parquet e delta. 
Na camada trusted, foi realizada a limpeza dos dados, trazendo a última data de processamento de cada registro. 
E por fim, na camada refined, foi realizada a ingestão com os dados tratados e prontos para consumo.

A orquestração foi definida usando o Airflow, usando o SparkSubmitOperator para realizar a conexão com o container do spark.

Foram consumidas api's de ações e api's com dados públicos, como dados da covid. 

Todo o projeto foi executado em uma máquina Ubuntu na AWS, usando o Lambda para programar o start-stop da VM.

O container docker foi aproveitado de um projeto executado pelo Rodrigo Azevedo. Link do repositório: https://github.com/razevedo1994/airflow_and_spark_docker_environment

# Screenshots

Exemplo de script de ingestão em Delta:

![image](https://user-images.githubusercontent.com/86599110/201266504-ddedecb2-9ffa-480a-af1f-3c94b9cfd01c.png)

Exemplo de dag: 

![image](https://user-images.githubusercontent.com/86599110/201266638-36b851d6-2516-4a77-9cfa-dda2fe6316c4.png)

Dag graph: 

![image](https://user-images.githubusercontent.com/86599110/201266761-faab6a97-ec74-46d8-b86b-1028ab9cab68.png)

Script Python criado com o AWS Lambda para o start-stop da vm no EC2.

![image](https://user-images.githubusercontent.com/86599110/201266933-a1646079-c22a-44cb-9d26-4b79bc687e5e.png)

