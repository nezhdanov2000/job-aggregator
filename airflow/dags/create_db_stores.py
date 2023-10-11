from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime


with DAG(
    dag_id='initial_create_db_stores',
    schedule_interval='@once',
    start_date=datetime(2023, 10, 5),
    catchup=False
) as dag:

    raw_store_name = Variable.get('raw_store_name')
    core_store_name = Variable.get('core_store_name')

    get_db_schemas = SQLExecuteQueryOperator(
        task_id='execute_query',
        conn_id='connection_vacancies_db',
        autocommit=True,
        database='vacancies_db',
        sql="""
      SELECT * from information_schema.schemata 
      WHERE NOT schema_name LIKE 'pg_%' AND schema_name <> 'information_schema'
    """,
        show_return_value_in_logs=True
    )

    def get_schemas_names(**kwargs):
        ti = kwargs['ti']
        db_schemas = ti.xcom_pull(task_ids='execute_query')
        schema_names = []
        try:
            schema_names = [schema_name[1] for schema_name in db_schemas]
        except:
            pass
        finally:
            return schema_names

    get_db_schemas_names = PythonOperator(
        task_id='get_db_schemas_names',
        python_callable=get_schemas_names,
        provide_context=True
    )

    with TaskGroup(group_id='group_create_raw_store') as group_create_raw_store:

        task_group_start = EmptyOperator(
            task_id="group_create_raw_store_start")

        task_group_end = EmptyOperator(
            task_id='group_create_raw_store_end'
        )

        create_raw_store = PostgresOperator(
            task_id='create_raw_store',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
          create schema {raw_store_name}
      """
        )

        create_raw_tables = PostgresOperator(
            task_id='create_raw_tables',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
          create table if not exists raw_store.vacancies_testsource (
            vacancy_id varchar,
            vacancy_parse_date date,
            vacancy_href varchar,
            vacancy_salary_min integer,
            vacancy_salary_max integer,
            vacancy_description varchar,
            vacancy_responsibilities varchar,
            vacancy_views integer,
            vacancy_source_name varchar,
            vacancy_source_href varchar,
            vacancy_name varchar,
            vacancy_category_name varchar,
            vacancy_experience varchar,
            vacancy_employer_name varchar,
            vacancy_schedule varchar,
            vacancy_education varchar,
            vacancy_placement date,
            vacancy_city varchar,
            vacancy_skills varchar,
            primary KEY(vacancy_id)
          );
        """
        )

        task_group_start >> create_raw_store >> create_raw_tables >> task_group_end

    with TaskGroup(group_id='group_create_core_store') as group_create_core_store:

        task_group_start = EmptyOperator(
            task_id="group_create_core_store_start")

        task_group_end = EmptyOperator(
            task_id='group_create_core_store_end'
        )

        create_core_store = PostgresOperator(
            task_id='create_core_store',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
          create schema {core_store_name}
      """
        )

        create_core_store_vacancies = PostgresOperator(
            task_id='create_core_store_vacancies',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies (
              vacancy_id varchar,
              vacancy_parse_date date,
              vacancy_href varchar,
              primary KEY(vacancy_id)
            );
          """
        )

        create_core_store_vacancies_salaries = PostgresOperator(
            task_id='create_core_store_vacancies_salaries',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_salaries (
              vacancy_id varchar,
              vacancy_salary_min integer,
              vacancy_salary_max integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null
            );
          """
        )

        create_core_store_vacancies_descriptions = PostgresOperator(
            task_id='create_core_store_vacancies_descriptions',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_descriptions (
              vacancy_id varchar,
              vacancy_description varchar,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null
            );
          """
        )

        create_core_store_vacancies_responsibilities = PostgresOperator(
            task_id='create_core_store_vacancies_responsibilities',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_responsibilities (
              vacancy_id varchar,
              vacancy_responsibility varchar,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null
            );
          """
        )

        create_core_store_vacancies_views = PostgresOperator(
            task_id='create_core_store_vacancies_views',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_views (
              vacancy_id varchar,
              vacancy_views integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null
            );
          """
        )

        create_core_store_sources = PostgresOperator(
            task_id='create_core_store_sources',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.sources (
              source_id serial,
              source_name varchar UNIQUE,
              source_href varchar,
              PRIMARY KEY(source_id)
            );
          """
        )

        create_core_store_vacancies_sources = PostgresOperator(
            task_id='create_core_store_vacancies_sources',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_sources (
              vacancy_id varchar,
              source_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (source_id) REFERENCES core_store.sources ON DELETE SET null
            );
          """
        )

        create_core_store_names = PostgresOperator(
            task_id='create_core_store_names',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.names (
              vacancy_name_id serial,
              vacancy_name varchar UNIQUE,
              PRIMARY KEY(vacancy_name_id)
            );
          """
        )

        create_core_store_vacancies_names = PostgresOperator(
            task_id='create_core_store_vacancies_names',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_names (
              vacancy_id varchar,
              vacancy_name_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_name_id) REFERENCES core_store.names ON DELETE SET null
            );
          """
        )

        create_core_store_categories = PostgresOperator(
            task_id='create_core_store_categories',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.categories (
              vacancy_category varchar UNIQUE,
              vacancy_category_id serial,
              PRIMARY KEY(vacancy_category_id)
            );
          """
        )

        create_core_store_vacancies_categories = PostgresOperator(
            task_id='create_core_store_vacancies_categories',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_categories (
              vacancy_id varchar,
              vacancy_category_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_category_id) REFERENCES core_store.categories ON DELETE SET null
            );
          """
        )

        create_core_store_experiences = PostgresOperator(
            task_id='create_core_store_experiences',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.experiences (
              vacancy_experience varchar UNIQUE,
              vacancy_experience_id serial,
              PRIMARY KEY(vacancy_experience_id)
            );
          """
        )

        create_core_store_vacancies_experiences = PostgresOperator(
            task_id='create_core_store_vacancies_experiences',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_experiences (
              vacancy_id varchar,
              vacancy_experience_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_experience_id) REFERENCES core_store.experiences ON DELETE SET null
            );
          """
        )

        create_core_store_employers = PostgresOperator(
            task_id='create_core_store_employers',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.employers (
              vacancy_employer varchar UNIQUE,
              vacancy_employer_id serial,
              PRIMARY KEY(vacancy_employer_id)
            );
          """
        )

        create_core_store_vacancies_employers = PostgresOperator(
            task_id='create_core_store_vacancies_employers',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_employers (
              vacancy_id varchar,
              vacancy_employer_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_employer_id) REFERENCES core_store.employers  ON DELETE SET null
            );
          """
        )

        create_core_store_schedules = PostgresOperator(
            task_id='create_core_store_schedules',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.schedules (
              vacancy_schedule varchar UNIQUE,
              vacancy_schedule_id serial,
              PRIMARY KEY(vacancy_schedule_id)
            );
          """
        )

        create_core_store_vacancies_schedules = PostgresOperator(
            task_id='create_core_store_vacancies_schedules',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_schedules (
              vacancy_id varchar,
              vacancy_schedule_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_schedule_id) REFERENCES core_store.schedules ON DELETE SET null
            );
          """
        )

        create_core_store_educations = PostgresOperator(
            task_id='create_core_store_educations',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.educations (
              vacancy_education varchar UNIQUE,
              vacancy_education_id serial,
              PRIMARY KEY(vacancy_education_id)
            );
          """
        )

        create_core_store_vacancies_educations = PostgresOperator(
            task_id='create_core_store_vacancies_educations',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_educations (
              vacancy_id varchar,
              vacancy_education_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_education_id) REFERENCES core_store.educations ON DELETE SET null
            );
          """
        )

        create_core_store_placements = PostgresOperator(
            task_id='create_core_store_placements',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.placements (
              vacancy_placement date UNIQUE,
              vacancy_placement_id serial,
              PRIMARY KEY(vacancy_placement_id)
            );
          """
        )

        create_core_store_vacancies_placements = PostgresOperator(
            task_id='create_core_store_vacancies_placements',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_placements (
              vacancy_id varchar,
              vacancy_placement_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_placement_id) REFERENCES core_store.placements ON DELETE SET null
            );
          """
        )

        create_core_store_cities = PostgresOperator(
            task_id='create_core_store_cities',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.cities (
              vacancy_city varchar UNIQUE,
              vacancy_city_id serial,
              PRIMARY KEY(vacancy_city_id)
            );
          """
        )

        create_core_store_vacancies_cities = PostgresOperator(
            task_id='create_core_store_vacancies_cities',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_cities (
              vacancy_id varchar,
              vacancy_city_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_city_id) REFERENCES core_store.cities ON DELETE SET null
            );
          """
        )

        create_core_store_skills = PostgresOperator(
            task_id='create_core_store_skills',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.skills (
              vacancy_skills varchar UNIQUE,
              vacancy_skills_id serial,
              PRIMARY KEY(vacancy_skills_id)
            );
          """
        )

        create_core_store_vacancies_skills = PostgresOperator(
            task_id='create_core_store_vacancies_skills',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.vacancies_skills (
              vacancy_id varchar,
              vacancy_skills_id integer,
              PRIMARY KEY(vacancy_id),
              FOREIGN KEY (vacancy_id) REFERENCES core_store.vacancies ON DELETE SET null,
              FOREIGN KEY (vacancy_skills_id) REFERENCES core_store.skills ON DELETE SET null
            );
          """
        )

        create_core_store_skills_ids = PostgresOperator(
            task_id='create_core_store_skills_ids',
            postgres_conn_id='connection_vacancies_db',
            autocommit=True,
            database='vacancies_db',
            sql=f"""
            create table if not exists core_store.skills_ids (
              skill_id serial,
              skill_name varchar UNIQUE,
              PRIMARY KEY(skill_id)
            );
          """
        )

        task_group_start >> create_core_store >> \
            create_core_store_vacancies >> \
            create_core_store_vacancies_salaries >> \
            create_core_store_vacancies_descriptions >> \
            create_core_store_vacancies_responsibilities >> \
            create_core_store_vacancies_views >> \
            create_core_store_sources >> create_core_store_vacancies_sources >> \
            create_core_store_names >> create_core_store_vacancies_names >> \
            create_core_store_categories >> create_core_store_vacancies_categories >> \
            create_core_store_experiences >> create_core_store_vacancies_experiences >> \
            create_core_store_employers >> create_core_store_vacancies_employers >> \
            create_core_store_schedules >> create_core_store_vacancies_schedules >> \
            create_core_store_educations >> create_core_store_vacancies_educations >> \
            create_core_store_placements >> create_core_store_vacancies_placements >> \
            create_core_store_cities >> create_core_store_vacancies_cities >> \
            create_core_store_skills >> create_core_store_vacancies_skills >> \
            create_core_store_skills_ids >> \
            task_group_end

    def check_schema_in_db(schema_name, **kwargs):
        ti = kwargs['ti']
        db_schemas = ti.xcom_pull(task_ids='get_db_schemas_names')
        if schema_name not in db_schemas:
            return f'group_create_{schema_name}.group_create_{schema_name}_start'
        else:
            return f'{schema_name}_done'

    branch_create_raw_store = BranchPythonOperator(
        task_id='branch_create_raw_store',
        python_callable=check_schema_in_db,
        op_kwargs={'schema_name': 'raw_store'},
        trigger_rule="all_success",
    )

    branch_create_core_store = BranchPythonOperator(
        task_id='branch_create_core_store',
        python_callable=check_schema_in_db,
        op_kwargs={'schema_name': 'core_store'},
        trigger_rule="all_success",
    )

    raw_store_done = EmptyOperator(
        task_id='raw_store_done',
        trigger_rule="all_success"
    )

    core_store_done = EmptyOperator(
        task_id='core_store_done',
        trigger_rule="all_success"
    )

    get_db_schemas >> get_db_schemas_names
    get_db_schemas_names >> \
        branch_create_raw_store >> group_create_raw_store >> raw_store_done
    branch_create_raw_store >> raw_store_done
    get_db_schemas_names >> \
        branch_create_core_store >> group_create_core_store >> core_store_done
    branch_create_core_store >> core_store_done
