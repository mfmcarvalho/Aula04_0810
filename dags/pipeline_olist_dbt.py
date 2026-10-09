"""
pipeline_olist_dbt — DESAFIO OURO (Cosmos + dbt)
O projeto dbt da Aula 3 (dags/dbt/olist) orquestrado pelo Airflow: cada modelo dbt
vira uma task, com lineage visível na UI. Usa schemas BRONZE / STAGING / SILVER / GOLD
(separados da DAG principal, que usa PUBLIC).

Depende do venv dbt criado no Dockerfile (/usr/local/airflow/dbt_venv) e do
astronomer-cosmos do requirements.txt. Mesma Connection snowflake_default.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, task
from cosmos import (DbtTaskGroup, ProjectConfig,
                    ProfileConfig, ExecutionConfig)
from cosmos.profiles import (SnowflakePrivateKeyPemProfileMapping,
                             SnowflakeUserPasswordProfileMapping)
 
DBT_PROJECT_DIR = str(Path(__file__).parent / 'dbt' / 'olist')   # = /usr/local/airflow/dags/dbt/olist no Astro
DBT_EXECUTABLE  = os.environ.get('DBT_EXECUTABLE', '/usr/local/airflow/dbt_venv/bin/dbt')  # venv criado no Dockerfile
 
PROFILE_ARGS = {'database': 'OLIST_LAB', 'schema': 'STAGING'}

# Como a Connection foi criada?  Etapa 13 (senha) ou Etapa 13B (par de chaves)?
# No Airflow 3 o DAG Processor não lê a Connection ao montar o grafo, então a escolha
# vem de uma variável de ambiente do Deployment (Astro > Deployment > Environment):
#   SNOWFLAKE_AUTH=keypair   -> usa extra.private_key_content
#   (ausente / password)     -> usa a senha da Connection
if os.environ.get('SNOWFLAKE_AUTH', 'password').lower() == 'keypair':
    _mapping = SnowflakePrivateKeyPemProfileMapping(conn_id='snowflake_default',
                                                    profile_args=PROFILE_ARGS)
else:
    _mapping = SnowflakeUserPasswordProfileMapping(conn_id='snowflake_default',
                                                   profile_args=PROFILE_ARGS)


profile_config = ProfileConfig(
    profile_name='olist',
    target_name='dev',
    profile_mapping=_mapping,
)
 
with DAG(
    dag_id='pipeline_olist_dbt',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['fiap', 'olist', 'aula4', 'ouro-dbt'],
) as dag:
 
    @task(task_id='extract_and_load_bronze')
    def extract_and_load_bronze():
        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
 
        BASE = ('https://raw.githubusercontent.com/'
                'rafsp/MBA_Aula2708/main/datasets/')
        arquivos = {
            'ORDERS':      'olist_orders_dataset.csv',
            'ORDER_ITEMS': 'olist_order_items_dataset.csv',
            'CUSTOMERS':   'olist_customers_dataset.csv',
            'PRODUCTS':    'olist_products_dataset.csv',
            'SELLERS':     'olist_sellers_dataset.csv',
            'PAYMENTS':    'olist_order_payments_dataset.csv',
            'REVIEWS':     'olist_order_reviews_dataset.csv',
        }
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute('CREATE DATABASE IF NOT EXISTS OLIST_LAB')
        cur.execute('CREATE SCHEMA IF NOT EXISTS OLIST_LAB.BRONZE')
        for tabela, nome in arquivos.items():
            df = pd.read_csv(BASE + nome)
            df.columns = [c.upper() for c in df.columns]
            write_pandas(conn, df, tabela, auto_create_table=True,
                         overwrite=True, database='OLIST_LAB',
                         schema='BRONZE')
            print(f'{tabela}: {len(df)} linhas')
        cur.close(); conn.close()
 
    transform = DbtTaskGroup(
        group_id='dbt_transform',
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE),
        operator_args={'install_deps': False},
    )
 
    @task(task_id='quality_checks')
    def quality_checks():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        total = hook.get_first(
            'SELECT COUNT(*) FROM OLIST_LAB.GOLD.FCT_RECEITA_ESTADO')[0]
        if total == 0:
            raise ValueError('Gold vazio — o pipeline falhou')
        print(f'OK - {total} estados na camada Gold')
 
    extract_and_load_bronze() >> transform >> quality_checks()