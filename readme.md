### job_portal

Запуск:   
`docker-compose up airflow-init`  
`docker-compose up`
***

Airflow DAGS:  
`set_vacancies_variables` - установка переменных и соединений `DONE`  
`create_db_store` - создание сущностей в БД `DONE`  
`load_data_to_db` - загрузка данных в БД `DONE`  
В процессе:  
Даги для парсеров  
Даг для запуска модели, очистки\обогащение данных

Схема БД:
![raw_store](./docs/ERDDiagram1.png)
