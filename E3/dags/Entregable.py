# Este es el DAG que orquesta el ETL de la tabla users

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.models import Variable

from datetime import datetime, timedelta

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS NBAstat3 (
    PLAYER_ID float distkey, 
    RANK float, PLAYER varchar,
    TEAM_ID float ,
    TEAM varchar,
    GP float,
    REB float,
    AST float,
    STL float,
    BLK float,
    TEAM_PLAYERS float,
     SUM_GP_TEAM float
    ) sortkey(TEAM);
"""

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM users WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""


# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


defaul_args = {
    "owner": "Eric",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="Entregable3_Eric",
    default_args=defaul_args,
    description="Entregable3_Eric",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Tareas
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    Extraer_info = SparkSubmitOperator(
        task_id="Extraer_info",
        application=f'{Variable.get("spark_scripts_dir")}/file1.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    Transformar_data = SparkSubmitOperator(
        task_id="Transformar_data",
        application=f'{Variable.get("spark_scripts_dir")}/file2.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    Enviar_Redshift = SparkSubmitOperator(
        task_id="Enviar_Redshift",
        application=f'{Variable.get("spark_scripts_dir")}/file3.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )
    Extraer_info >> create_table >> Transformar_data >> Enviar_Redshift
    #get_process_date_task >> create_table >> clean_process_date >> Extraer_info >> Transformar_data>> Enviardata_Redshift
