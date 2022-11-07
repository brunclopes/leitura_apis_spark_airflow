from airflow import DAG
from pathlib import Path
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Arquivos jar para a ingestão no s3 e em Delta
folder = "trusted"
jars = "/opt/airflow/plugins/hadoop-aws-3.3.2.jar" "," "/opt/airflow/plugins/aws-java-sdk-bundle-1.11.1026.jar"\
                "," "/opt/airflow/plugins/delta-core_2.12-2.1.0.jar" "," "/opt/airflow/plugins/delta-storage-2.1.0.jar"

default_args = {
    "owner": "Bruno Lopes",
    "start_date": datetime(2022, 11, 7),
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    Path(__file__).stem,
    default_args=default_args,
    catchup=False,
    schedule_interval="0 22 * * *",
    max_active_runs=1,
    concurrency=3,
) as dag:

    start = DummyOperator(task_id="Begin")

    t1 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Semanal_Alphavantage_Amazon"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_semanal_alphavantage_amazon.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t2 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Semanal_Alphavantage_Ambev"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_semanal_alphavantage_ambev.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )    

    t3 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Semanal_Alphavantage_Bradesco"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_semanal_alphavantage_bradesco.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )    

    t4 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Semanal_Alphavantage_Itau"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_semanal_alphavantage_itau.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )  

    t5 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Semanal_Alphavantage_Micosoft"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_semanal_alphavantage_microsoft.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )        

    t6 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Diaria_Alphavantage_Amazon"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_diaria_alphavantage_amazon.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t7 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Diaria_Alphavantage_Ambev"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_diaria_alphavantage_ambev.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )    

    t8 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Diaria_Alphavantage_Bradesco"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_diaria_alphavantage_bradesco.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t9 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Diaria_Alphavantage_Itau"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_diaria_alphavantage_itau.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t10 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Diaria_Alphavantage_Microsoft"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_diaria_alphavantage_microsoft.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    end = DummyOperator(task_id="Ending")

    start >> t1 >> t2 >> t3 >> t4 >> t5 >> end 
    start >> t6 >> t7 >> t8 >> t9 >> t10 >> end
