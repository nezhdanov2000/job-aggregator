import pandas as pd
from airflow import DAG
# from airflow.operators.sql import BranchSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.base_hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import settings
from airflow.models import Connection
from airflow.models import Variable
from datetime import datetime
from sqlalchemy import create_engine
import requests
import psycopg2
# import feedparser
import os

import requests
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import os

with DAG(
    dag_id='load_data_to_db',
    schedule_interval='@once',
    start_date=datetime(2023, 10, 5),
    catchup=False
) as dag:

    raw_store_name = Variable.get('raw_store_name')
    core_store_name = Variable.get('core_store_name')
    raw_data_path = Variable.get('raw_data_path')
    raw_data_folder_name = Variable.get('raw_data_folder_name')
    store_connection_params = BaseHook.get_connection(
        'connection_vacancies_db')
    db_name = Variable.get('db_name')

    def psycopg2_connection(store_connection_params, db_name):
        try:
            store_connection = psycopg2.connect(
                dbname=db_name,
                user=store_connection_params.login,
                password=store_connection_params.password,
                host=store_connection_params.host,
                port=store_connection_params.port)
            return store_connection
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def fn_task_get_file_path(data_path, **kwargs):
        files = os.listdir(data_path)
        file_path = os.path.join(data_path, files[0])
        kwargs['ti'].xcom_push(
            value=files[0], key='current_file_name')
        kwargs['ti'].xcom_push(
            value=file_path, key='current_file_path')

    def fn_create_skill_id_table_in_db(store_connection, **kwargs):
        current_skills_list_from_core = [el[0] for el in kwargs['ti'].xcom_pull(
            task_ids='group_create_skill_id_tables_in_core_store.get_current_skills_from_core_db')]
        raw_skills_arrays = [skills[1].split(',') for skills in kwargs['ti'].xcom_pull(
            task_ids='group_create_skill_id_tables_in_core_store.get_current_skills_from_raw_db'
        )]

        current_skills_set_from_raw = set()
        for skills in raw_skills_arrays:
            for skill in skills:
                current_skills_set_from_raw.add(skill.lower().strip())

        new_skills = [
            skill for skill in current_skills_set_from_raw if skill not in current_skills_list_from_core]

        if len(new_skills) > 0:

            store_cursor = store_connection.cursor()
            for skill in new_skills:
                sql_query_insert_new_skill_to_skills_ids = f"""
                  insert into {core_store_name}.skills_ids (skill_name)
                  values ('{skill}')
                  ON conflict (skill_name) DO NOTHING
                  RETURNING skill_id;
                """
                store_cursor.execute(sql_query_insert_new_skill_to_skills_ids)
                store_connection.commit()
                skill_id = store_cursor.fetchone()[0]

                sql_query_create_new_table_skill_id = f"""
                  CREATE TABLE IF NOT EXISTS {core_store_name}.skill_{skill_id} (
                  vacancy_id varchar,
                  PRIMARY KEY(vacancy_id),
                  FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null
                  );
                """
                store_cursor.execute(sql_query_create_new_table_skill_id)
                store_connection.commit()

            store_connection.close()

    def fn_load_data_from_file_to_raw_store(store_connection, **kwargs):
        current_file_path = kwargs['ti'].xcom_pull(
            key='current_file_path')

        df = pd.read_csv(current_file_path, encoding="utf-8",
                         on_bad_lines='skip',
                         header=0,
                         sep=';')

        store_cursor = store_connection.cursor()
        # insert into {raw_store_name}.{current_file_name} (
        for i, row in enumerate(df.itertuples(), 1):
            sql_query_to_raw = f"""
                insert into {raw_store_name}.vacancies_testsource (
                vacancy_id,
                vacancy_parse_date,
                vacancy_href,
                vacancy_salary_min,
                vacancy_salary_max,
                vacancy_description,
                vacancy_responsibilities,
                vacancy_views,
                vacancy_source_name,
                vacancy_source_href,
                vacancy_name,
                vacancy_category_name,
                vacancy_experience,
                vacancy_employer_name,
                vacancy_schedule,
                vacancy_education,
                vacancy_placement,
                vacancy_city,
                vacancy_skills
                ) VALUES (
                  '{row.vacancy_id}',
                  '{row.vacancy_parse_date}',
                  '{row.vacancy_href}',
                  {row.vacancy_salary_min},
                  {row.vacancy_salary_max},
                  '{row.vacancy_description}',
                  '{row.vacancy_responsibilities}',
                  {row.vacancy_views},
                  '{row.vacancy_source_name}',
                  '{row.vacancy_source_href}',
                  '{row.vacancy_name}',
                  '{row.vacancy_category_name}',
                  '{row.vacancy_experience}',
                  '{row.vacancy_employer_name}',
                  '{row.vacancy_schedule}',
                  '{row.vacancy_education}',
                  '{row.vacancy_placement}',
                  '{row.vacancy_city}',
                  '{row.vacancy_skills}'
                ) ON CONFLICT DO nothing
            """
            store_cursor.execute(sql_query_to_raw)
            store_connection.commit()

        store_connection.close()

    def fn_delete_file(**kwargs):
        current_file_path = kwargs['ti'].xcom_pull(
            key='current_file_path')
        os.remove(current_file_path)

    def fn_load_data_from_raw_to_core_vacancies_skill_id(store_connection, **kwargs):

        raw_skills_arrays = kwargs['ti'].xcom_pull(
            task_ids='group_create_skill_id_tables_in_core_store.get_current_skills_from_raw_db')

        store_cursor = store_connection.cursor()

        for skills in raw_skills_arrays:
            vacancy_id = skills[0]
            skills_arr = skills[1].split(',')
            for skill in skills_arr[1:]:
                sql_query_get_skill_id = f"""
                  select 
                    skill_id 
                  from {core_store_name}.skills_ids
                  where skill_name = '{skill.strip()}'
                """
                store_cursor.execute(sql_query_get_skill_id)
                store_connection.commit()
                skill_id = store_cursor.fetchone()[0]

                sql_query_insert_to_skill_id = f"""
                  insert into {core_store_name}.skill_{skill_id}
                    VALUES ('{vacancy_id.strip()}')
                  ON conflict (vacancy_id) DO NOTHING;
                """
                store_cursor.execute(sql_query_insert_to_skill_id)
                store_connection.commit()

        store_connection.close()

    # TASKs
    task_wait_files = FileSensor(
        task_id=f'wait_files',
        fs_conn_id='filepath',
        filepath=f'{raw_data_path}/{raw_data_folder_name}',
        poke_interval=10
    )

    task_get_file_path = PythonOperator(
        task_id=f'get_file_path',
        python_callable=fn_task_get_file_path,
        op_kwargs={'data_path': f"{raw_data_path}/{raw_data_folder_name}"}
    )

    with TaskGroup(group_id='group_load_from_file_to_raw_store') as group_load_from_file_to_raw_store:

        task_group_start = EmptyOperator(task_id="group_load_start")
        task_group_end = EmptyOperator(task_id="group_load_end")

        task_load_data_from_file_to_raw_store = PythonOperator(
            task_id=f'load_data_from_file_to_raw_store',
            python_callable=fn_load_data_from_file_to_raw_store,
            op_kwargs={'store_connection': psycopg2_connection(
                store_connection_params, db_name)}
        )

        task_group_start >> task_load_data_from_file_to_raw_store >> task_group_end

    with TaskGroup(group_id='group_create_skill_id_tables_in_core_store') as group_create_skill_id_tables_in_core_store:

        task_group_start = EmptyOperator(task_id="group_load_start")
        task_group_end = EmptyOperator(task_id="group_load_end")

        task_get_current_skills_from_core_db = SQLExecuteQueryOperator(
            task_id='get_current_skills_from_core_db',
            conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              SELECT skill_name from {core_store_name}.skills_ids
            """,
            show_return_value_in_logs=True
        )

        task_get_current_skills_from_raw_db = SQLExecuteQueryOperator(
            task_id='get_current_skills_from_raw_db',
            conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              SELECT distinct vacancy_id, vacancy_skills from raw_store.vacancies_testsource
            """,
            show_return_value_in_logs=True
        )

        task_create_skill_id_tables_in_db = PythonOperator(
            task_id=f'create_skill_id_table_in_db',
            python_callable=fn_create_skill_id_table_in_db,
            op_kwargs={'store_connection': psycopg2_connection(
                store_connection_params, db_name)}
        )

        task_group_start >> \
            task_get_current_skills_from_core_db >> \
            task_get_current_skills_from_raw_db >> \
            task_create_skill_id_tables_in_db >> \
            task_group_end

    with TaskGroup(group_id='group_load_data_to_core_store') as group_load_data_to_core_store:

        task_group_start = EmptyOperator(task_id="group_load_start")
        task_group_end = EmptyOperator(task_id="group_load_end")

        task_load_data_from_raw_to_core_vacancies = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies (
                vacancy_id,
                vacancy_parse_date,
                vacancy_href
              )
                select
                  vacancy_id, vacancy_parse_date, vacancy_href
                from raw_store.vacancies_testsource
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_salaries = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_salaries',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_salaries (
                vacancy_id,
                vacancy_salary_min,
                vacancy_salary_max
              )
                select
                  vacancy_id, vacancy_salary_min, vacancy_salary_max
                from raw_store.vacancies_testsource
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_descriptions = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_descriptions',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_descriptions (
                vacancy_id,
                vacancy_description
              )
                select
                  vacancy_id, vacancy_description
                from raw_store.vacancies_testsource
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_responsibilities = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_responsibilities',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_responsibilities (
                vacancy_id,
                vacancy_responsibility
              )
                select
                  vacancy_id, vacancy_responsibilities
                from raw_store.vacancies_testsource
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_views = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_views',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_views (
                vacancy_id,
                vacancy_views
              )
                select
                  vacancy_id, vacancy_views
                from raw_store.vacancies_testsource
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_sources = PostgresOperator(
            task_id='load_data_from_raw_to_core_sources',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.sources (
                source_name,
                source_href
              )
                select
                  vacancy_source_name, vacancy_source_href
                from raw_store.vacancies_testsource
              ON conflict (source_name) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_sources = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_sources',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_sources (
                vacancy_id,
                source_id
              )
                (select
                  vacancy_id,
                  (select
                    source_id
                  from {core_store_name}.sources
                  where raw_store.vacancies_testsource.vacancy_source_name = {core_store_name}.sources.source_name)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_names = PostgresOperator(
            task_id='load_data_from_raw_to_core_names',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.names (
                vacancy_name
              )
                select
                  vacancy_name
                from raw_store.vacancies_testsource
              ON conflict (vacancy_name) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_names = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_names',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_names (
                vacancy_id,
                vacancy_name_id
              )
                (select
                  vacancy_id,
                  (select
                    vacancy_name_id
                  from {core_store_name}.names
                  where raw_store.vacancies_testsource.vacancy_name = {core_store_name}.names.vacancy_name)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_categories = PostgresOperator(
            task_id='load_data_from_raw_to_core_categories',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.categories (
                vacancy_category
              )
                select
                  vacancy_category_name
                from raw_store.vacancies_testsource
              ON conflict (vacancy_category) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_categories = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_categories',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_categories (
                vacancy_id,
                vacancy_category_id
              )
                (select
                  vacancy_id,
                  (select
                    vacancy_category_id
                  from {core_store_name}.categories
                  where raw_store.vacancies_testsource.vacancy_category_name = {core_store_name}.categories.vacancy_category)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_experiences = PostgresOperator(
            task_id='load_data_from_raw_to_core_experiences',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.experiences (
                vacancy_experience
              )
                select
                  vacancy_experience
                from raw_store.vacancies_testsource
              ON conflict (vacancy_experience) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_experiences = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_experiences',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_experiences (
                vacancy_id,
                vacancy_experience_id
              )
                (select
                  vacancy_id,
                  (select
                    vacancy_experience_id
                  from {core_store_name}.experiences
                  where raw_store.vacancies_testsource.vacancy_experience = {core_store_name}.experiences.vacancy_experience)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_employers = PostgresOperator(
            task_id='load_data_from_raw_to_core_employers',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.employers (
                vacancy_employer
              )
                select
                  vacancy_employer_name
                from raw_store.vacancies_testsource
              ON conflict (vacancy_employer) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_employers = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_employers',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_employers (
                vacancy_id,
                vacancy_employer_id
              )
                (select
                  vacancy_id,
                  (select
                    vacancy_employer_id
                  from {core_store_name}.employers
                  where raw_store.vacancies_testsource.vacancy_employer_name = {core_store_name}.employers.vacancy_employer)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_schedules = PostgresOperator(
            task_id='load_data_from_raw_to_core_schedules',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.schedules (
                vacancy_schedule
              )
                select
                  vacancy_schedule
                from raw_store.vacancies_testsource
              ON conflict (vacancy_schedule) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_schedules = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_schedules',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_schedules (
                vacancy_id,
                vacancy_schedule_id
              )
                (select
                  vacancy_id,
                  (select
                    vacancy_schedule_id
                  from {core_store_name}.schedules
                  where raw_store.vacancies_testsource.vacancy_schedule = {core_store_name}.schedules.vacancy_schedule)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_educations = PostgresOperator(
            task_id='load_data_from_raw_to_core_educations',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.educations (
                vacancy_education
              )
                select
                  vacancy_education
                from raw_store.vacancies_testsource
              ON conflict (vacancy_education) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_educations = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_educations',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_educations (
                vacancy_id,
                vacancy_education_id
              )
                (select
                  vacancy_id,
                  (select
                    vacancy_education_id
                  from {core_store_name}.educations
                  where raw_store.vacancies_testsource.vacancy_education = {core_store_name}.educations.vacancy_education)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_placements = PostgresOperator(
            task_id='load_data_from_raw_to_core_placements',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.placements (
                vacancy_placement
              )
                select
                   vacancy_placement
                from raw_store.vacancies_testsource
              ON conflict (vacancy_placement) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_placements = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_placements',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_placements (
                vacancy_id,
                vacancy_placement_id
              )
                (select
                  vacancy_id,
                  (select
                     vacancy_placement_id
                  from {core_store_name}.placements
                  where raw_store.vacancies_testsource.vacancy_placement = {core_store_name}.placements.vacancy_placement)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_cities = PostgresOperator(
            task_id='load_data_from_raw_to_core_cities',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.cities (
                vacancy_city
              )
                select
                   vacancy_city
                from raw_store.vacancies_testsource
              ON conflict (vacancy_city) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_cities = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_cities',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_cities (
                vacancy_id,
                vacancy_city_id
              )
                (select
                  vacancy_id,
                  (select
                     vacancy_city_id
                  from {core_store_name}.cities
                  where raw_store.vacancies_testsource.vacancy_city = {core_store_name}.cities.vacancy_city)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_skills = PostgresOperator(
            task_id='load_data_from_raw_to_core_skills',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.skills (
                vacancy_skills
              )
                select
                   vacancy_skills
                from raw_store.vacancies_testsource
              ON conflict (vacancy_skills) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_skills = PostgresOperator(
            task_id='load_data_from_raw_to_core_vacancies_skills',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
              insert into {core_store_name}.vacancies_skills (
                vacancy_id,
                vacancy_skills_id
              )
                (select
                  vacancy_id,
                  (select
                     vacancy_skills_id
                  from {core_store_name}.skills
                  where raw_store.vacancies_testsource.vacancy_skills = {core_store_name}.skills.vacancy_skills)
                from raw_store.vacancies_testsource)
              ON conflict (vacancy_id) DO NOTHING
            """
        )

        task_load_data_from_raw_to_core_vacancies_skill_id = PythonOperator(
            task_id=f'load_data_from_raw_to_core_vacancies_skill_id',
            python_callable=fn_load_data_from_raw_to_core_vacancies_skill_id,
            op_kwargs={'store_connection': psycopg2_connection(
                store_connection_params, db_name)}
        )

        task_group_start >> \
            task_load_data_from_raw_to_core_vacancies >> \
            task_load_data_from_raw_to_core_vacancies_salaries >> \
            task_load_data_from_raw_to_core_vacancies_descriptions >> \
            task_load_data_from_raw_to_core_vacancies_responsibilities >> \
            task_load_data_from_raw_to_core_vacancies_views >> \
            task_load_data_from_raw_to_core_sources >> \
            task_load_data_from_raw_to_core_vacancies_sources >> \
            task_load_data_from_raw_to_core_names >> \
            task_load_data_from_raw_to_core_vacancies_names >> \
            task_load_data_from_raw_to_core_categories >> \
            task_load_data_from_raw_to_core_vacancies_categories >> \
            task_load_data_from_raw_to_core_experiences >> \
            task_load_data_from_raw_to_core_vacancies_experiences >> \
            task_load_data_from_raw_to_core_employers >> \
            task_load_data_from_raw_to_core_vacancies_employers >> \
            task_load_data_from_raw_to_core_schedules >> \
            task_load_data_from_raw_to_core_vacancies_schedules >> \
            task_load_data_from_raw_to_core_educations >> \
            task_load_data_from_raw_to_core_vacancies_educations >> \
            task_load_data_from_raw_to_core_placements >> \
            task_load_data_from_raw_to_core_vacancies_placements >> \
            task_load_data_from_raw_to_core_cities >> \
            task_load_data_from_raw_to_core_vacancies_cities >> \
            task_load_data_from_raw_to_core_skills >> \
            task_load_data_from_raw_to_core_vacancies_skills >> \
            task_load_data_from_raw_to_core_vacancies_skill_id >> \
            task_group_end

    task_delete_file = PythonOperator(
        task_id=f'delete_file',
        python_callable=fn_delete_file
    )

    task_wait_files >> task_get_file_path >> \
        group_load_from_file_to_raw_store >> \
        group_create_skill_id_tables_in_core_store >> \
        group_load_data_to_core_store >> \
        task_delete_file
