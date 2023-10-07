from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.hooks.base_hook import BaseHook
from airflow import settings
from airflow.models import Connection
from airflow.models import Variable
import datetime as dt

try:
    Variable.setdefault(key='raw_data_path', default='/opt/airflow/raw_data')
    Variable.setdefault(key='raw_data_folder_name', default='vacancies')
    Variable.setdefault(key='db_name', default='vacancies_db')
    Variable.setdefault(key='raw_store_name', default='raw_store')
    Variable.setdefault(key='core_store_name', default='core_store')

    try:
        session = settings.Session()
        conn = Connection(
            conn_id='connection_vacancies_db',
            conn_type='postgres',
            host='db',
            login='postgres',
            password='password',
            schema='postgres',
            port=5432
        )
        session.add(conn)
        session.commit()
    except Exception as ex:
        print(ex)

    try:
        session = settings.Session()
        conn = Connection(
            conn_id='filepath',
            conn_type='fs',
            extra='{"path":"/opt/airflow"}'
        )
        session.add(conn)
        session.commit()
    except Exception as ex:
        print(ex)

except:
    pass


# store_connection_params = BaseHook.get_connection('connection_vacancies_db')
# fs_connection_params = BaseHook.get_connection('filepath')
# db_name = Variable.get('db_name')
# raw_store_name = Variable.get('raw_store_name')
# core_store_name = Variable.get('core_store_name')
# mart_store_name = Variable.get('mart_store_name')
# raw_data_path = Variable.get('raw_data_path')
# raw_data_folder_name = Variable.get('raw_data_folder_name')

set_vacancies_variables = DAG(
    dag_id='set_vacancies_variables',
    start_date=dt.datetime(2021, 12, 1),
    schedule_interval='@once'
)

task_start = DummyOperator(task_id='start', dag=set_vacancies_variables)

task_start
