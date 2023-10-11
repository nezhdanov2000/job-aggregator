from faker import Faker
import string
import pandas as pd
import random

fake = Faker(['ru-RU'])
Faker.seed(random.random())

fake_domain_name = fake.domain_name()

dict_names_types = {
    'vacancy_id': {
        'type': 'str',
        'length': 12
    },
    'vacancy_parse_date': {
        'type': 'date'
    },
    'vacancy_href': {
        'type': 'not_random',
        'value': f'{fake_domain_name}/source_href'
    },
    'vacancy_salary_min': {
        'type': 'int',
        'length': 5
    },
    'vacancy_salary_max': {
        'type': 'int',
        'length': 5,
    },
    'vacancy_description': {
        'type': 'paragraph',
        'length': 5
    },
    'vacancy_responsibilities': {
        'type': 'paragraph',
        'length': 5
    },
    'vacancy_views': {
        'type': 'int',
        'length': 5,
    },
    'vacancy_source_name': {
        'type': 'not_random',
        'value': fake_domain_name
    },
    'vacancy_source_href': {
        'type': 'not_random',
        'value': f'{fake_domain_name}/vacancy_href'
    },
    'vacancy_name': {
        'type': 'from_array',
        'array': ['Data engineer', 'Data Scientist', 'Data Analytic']
    },
    'vacancy_category_name': {
        'type': 'not_random',
        'value': 'category'
    },
    'vacancy_experience': {
        'type': 'from_array',
        'array': ['Удаленка', 'Гибрид', 'Офис', 'Не указано']
    },
    'vacancy_employer_name': {
        'type': 'company'
    },
    'vacancy_schedule': {
        'type': 'from_array',
        'array': ['Без опыта', 'от 1 года', 'от 3 лет']
    },
    'vacancy_education': {
        'type': 'from_array',
        'array': ['Высшее', 'Среднее специальное', 'Не оконченное высшее', 'Не указано']
    },
    'vacancy_placement': {
        'type': 'date'
    },
    'vacancy_city': {
        'type': 'city'
    },
    'vacancy_skills': {
        'type': 'some_from_array',
        'array': ['python', 'postgres', 'sql', 'scala', 'greenplum', 'clickhouse', 'mysql', 'pandas', 'spark', 'hadoop', 'java', 'c#', 'opencv', 'tensorflow', 'matplotlib', 'pytorch']
    }
}


def return_random_string(length):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def return_random_integer(length):
    return int(''.join(random.choice(string.digits) for _ in range(length)))


def return_random_date():
    return fake.date_time_between(start_date='-30d', end_date='now').strftime("%Y-%m-%d")


def return_random_paragraph(length):
    return fake.paragraph(nb_sentences=length).replace("'", "`")


def return_random_company():
    return fake.large_company().replace("'", "`")


def return_random_city():
    return fake.city_name()


def generate_data_arr_of_dicts(dict_names_types, arr_length):

    fake_arr_of_dicts = []
    for row_num in range(arr_length):
        row = {}
        for key, value in dict_names_types.items():
            if value['type'] == 'str':
                row[key] = f"{return_random_string(value['length']) + str(row_num)}"
                continue
            if value['type'] == 'int':
                row[key] = return_random_integer(value['length'])
                continue
            if value['type'] == 'date':
                row[key] = return_random_date()
                continue
            if value['type'] == 'paragraph':
                row[key] = return_random_paragraph(value['length'])
                continue
            if value['type'] == 'not_random':
                row[key] = value['value']
                continue
            if value['type'] == 'company':
                row[key] = return_random_company()
                continue
            if value['type'] == 'from_array':
                row[key] = random.choice(value['array'])
                continue
            if value['type'] == 'some_from_array':
                row[key] = random.sample(
                    value['array'], random.randint(1, len(value['array'])))
                continue
            if value['type'] == 'city':
                row[key] = return_random_city()
                continue
        fake_arr_of_dicts.append(row)

    return fake_arr_of_dicts


def split_vacancies_skills(data_arr_of_dicts):
    vacancies_arr = []
    for vacancy in data_arr_of_dicts:
        row = {**vacancy}
        row['vacancy_skills'] = ', '.join(row['vacancy_skills'])
        vacancies_arr.append(row)

    return vacancies_arr


df = pd.DataFrame(split_vacancies_skills(
    generate_data_arr_of_dicts(dict_names_types, 100000)))

df.to_csv(f'./airflow/raw_data/vacancies/fake_data.csv',
          sep=';', encoding='utf-8-sig')
