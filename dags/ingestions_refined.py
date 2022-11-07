from airflow import DAG
from pathlib import Path
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Arquivos jar para a ingestão fno s3 e em Delta
folder = 'refined'
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
    schedule_interval="30 22 * * *",
    max_active_runs=1,
    concurrency=3,
) as dag:

    start = DummyOperator(task_id="Begin")


    t1 = SparkSubmitOperator(
         task_id="Ingestao_Camara_Deputados"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_camara_deputados.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t2 = SparkSubmitOperator(
         task_id="Ingestao_Covid_Brasil"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_covid_brasil.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t3 = SparkSubmitOperator(
         task_id="Ingestao_Bacen_Dinheiro_Circulacao"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_bacen_dinheiro_circulacao.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )
    
    t4 = SparkSubmitOperator(
         task_id="Ingestao_Bancos_Brasil"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_bancos_brasil.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t5 = SparkSubmitOperator(
         task_id="Ingestao_Feriados_Brasil"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_feriados.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t6 = SparkSubmitOperator(
         task_id="Ingestao_Taxas_Brasil"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_taxas.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t7 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Diaria_Alphavantage"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_diaria_alphavantage.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    t8 = SparkSubmitOperator(
         task_id="Ingestao_Cotacao_Semanai_Alphavantage"
        ,application=f"/opt/airflow/dags/scripts/{folder}/api_cotacao_semanal_alphavantage.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    end = DummyOperator(task_id="Ending")

    start >> t1 >> t2 >> t3 >> end
    start >> t4 >> t5 >> t6 >> end
    start >> t7 >> t8 >> end
