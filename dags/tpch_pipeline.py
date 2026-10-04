from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'tpch_medallion_pipeline',
    default_args=default_args,
    description='Pipeline Medallion para TPCH (Bronze -> Silver -> Gold)',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['spark', 'delta', 'tpch'],
) as dag:

    # =========================================================================
    # TAREAS
    # =========================================================================
    
    # 1. Capa Bronze (Ingesta Incremental mediante Left Anti Join)
    bronze_task = BashOperator(
        task_id='ingest_bronze',
        bash_command='python3 /workspace/jobs/run_bronze.py /workspace/jobs/silver_config.yml'
    )

    # 2. Capa Silver (Tipado, Reglas de Negocio, Limpieza)
    silver_task = BashOperator(
        task_id='process_silver',
        bash_command='python3 /workspace/jobs/silver_processor.py /workspace/jobs/silver_config.yml'
    )

    # 3. Capa Gold (Modelo Estrella, Agregaciones)
    gold_task = BashOperator(
        task_id='process_gold',
        bash_command='python3 /workspace/jobs/gold_processor.py /workspace/jobs/gold_config.yml'
    )

    # =========================================================================
    # ORQUESTACIÓN (MALLA / DEPENDENCIAS)
    # =========================================================================
    
    bronze_task >> silver_task >> gold_task
