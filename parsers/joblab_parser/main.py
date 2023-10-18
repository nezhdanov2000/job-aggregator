from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
import time
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests
from tqdm import tqdm
import logging
import os

# Настройка логирования
log_filename = 'parser_log.txt'
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(filename=log_filename, level=logging.INFO, format=log_format)


# Создайте список свойств для отслеживания
properties = ['job_title', 'employer', 'city', 'salary', 'schedule', 'conditions', 'responsibilities', 'education',
              'experience', 'requirements', 'placement', 'job_views_value']
# Создайте словарь для хранения информации о том, было ли извлечение свойства успешным
property_parsed = {}
# Устанавливаем флаги для каждого значения
for property_name in properties:
    property_parsed[property_name] = False

max_pages_to_parse = 0  # Измените это значение на желаемое количество страниц

# Путь к директории, в которой должен сохраняться файл
output_directory = './parsers/joblab_parser'

# Проверяем, существует ли директория, и если нет, создаем ее
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# options
options = webdriver.ChromeOptions()
useragent = UserAgent()
options.add_argument(f'user-agent={useragent.random}')

# headles mode
# options.add_argument("--headless")

# disable webdriver
options.add_argument("--disable-extensions")
options.add_argument("--disable-extensions-file-access-check")
options.add_argument("--disable-extensions-http-throttling")
options.add_argument("--disable-infobars")
options.add_argument("--disable-web-security")
options.add_argument("--disable-blink-features=AutomationControlled")

driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
# driver = webdriver.Chrome(ChromeDriverManager().install())

parse_datetime = datetime.now()
datetime_to_filename = f"{parse_datetime.strftime('%d-%m-%Y_%H-%M-%S')}"
page_num = 1
url = f'https://joblab.ru/search.php?r=vac&view=short&srcategory%5B%5D=16&srregion=100&maxThread=90&page={page_num}&submit=1'


def get_page_html(url):
    driver.get(url=url)
    time.sleep(1)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return soup


def get_vacancies_hrefs(soup):
    vacancies = soup.find_all("p", class_="prof")
    parsed_data = []
    if len(vacancies) > 0:
        for el in soup.find_all("p", class_="prof"):
            data_row = {}
            data_row['vacancy_href'] = f"https://joblab.ru{el.find('a')['href']}"
            data_row['vacancy_href_parse_date'] = parse_datetime.strftime("%d/%m/%Y %H:%M:%S")
            parsed_data.append(data_row)
    return parsed_data


vacancies = get_vacancies_hrefs(get_page_html(url))

try:
    vacancies = get_vacancies_hrefs(get_page_html(url))
    df = pd.DataFrame(vacancies)
    df.to_csv(f'./parsers/joblab_parser/joblab_hrefs_{datetime_to_filename}.csv',
              sep='|',
              encoding='utf-8-sig',
              mode='w',
              header=vacancies[0].keys(),
              index=False
              )
    logging.info(f'parsed: page {page_num}')

    while (len(vacancies) > 0) and (page_num <= max_pages_to_parse):
        page_num = page_num + 1
        url = f'https://joblab.ru/search.php?r=vac&view=short&srcategory%5B%5D=16&srregion=100&maxThread=90&page={page_num}&submit=1'
        vacancies = get_vacancies_hrefs(get_page_html(url))
        df = pd.DataFrame(vacancies)
        df.to_csv(f'./parsers/joblab_parser/joblab_hrefs_{datetime_to_filename}.csv',
                  sep='|',
                  encoding='utf-8-sig',
                  mode='a',
                  header=False,
                  index=False
                  )
        logging.info(f'parsed: page {page_num}\r')

except Exception as e:
    logging.error(e)
finally:
    driver.close()
    driver.quit()

# ///////////////////////////////////////

month_dict = {
    'января': '01',
    'февраля': '02',
    'марта': '03',
    'апреля': '04',
    'мая': '05',
    'июня': '06',
    'июля': '07',
    'августа': '08',
    'сентября': '09',
    'октября': '10',
    'ноября': '11',
    'декабря': '12'
}

try:
    df = pd.read_csv(f'./parsers/joblab_parser/joblab_hrefs_{datetime_to_filename}.csv', sep='|')
    parsed_data = []

    for href in tqdm(df['vacancy_href']):

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        response = requests.get(
            href,
            headers=headers,
        )

        if response.status_code == 200:
            vacancy = {}

            soup = BeautifulSoup(response.text, 'lxml')

            vacancy_table = soup.find('table', class_="table-to-div")

            #                 Ссылка на вакансию (job_href)
            #                 Название вакансии (job_title)
            #                 Работодатель  (employer)
            #                 Город (city)
            #                 Заработная плата (salary)
            #                 График работы (schedule)
            #                 Условия (conditions)
            #                 Обязанности (responsibilities)
            #                 Образование (education)
            #                 Опыт работы (experience)
            #                 Требования (requirements)
            #                 Время размещения вакансии (placement)
            #                 Количество просмотров (job_views_value)
            #                 Укороченное описание вакансии (NULL)

            # Ссылка на вакансию
            vacancy['job_href'] = href

            # Наименование вакансии
            try:
                vacancy['job_title'] = soup.find('h1').text
                property_parsed['job_title'] = True
            except Exception as e:
                vacancy['job_title'] = ""
                error_message = f"Information about job_title not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Работодатель
            try:
                vacancy['employer'] = vacancy_table.find_all('a')[0].text
                property_parsed['employer'] = True
            except Exception as e:
                vacancy['employer'] = ""
                error_message = f"Information about employer not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Город
            try:
                vacancy['city'] = vacancy_table.find_all('b')[1].text
                property_parsed['city'] = True
            except Exception as e:
                vacancy['city'] = ""
                error_message = f"Information about city not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Зарплата
            try:
                vacancy['salary'] = vacancy_table.find_all('b')[-1].text
                property_parsed['salary'] = True
            except Exception as e:
                vacancy['salary'] = ""
                error_message = f"Information about salary not found: {href}. Error details: {e}"
                logging.error(error_message)

            # График работы
            try:
                vacancy['schedule'] = vacancy_table.find('p', string="График работы").parent.parent.find_all('p')[
                    -1].text
                property_parsed['schedule'] = True
            except Exception as e:
                vacancy['schedule'] = ""
                error_message = f"Information about schedule not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Условия работы
            try:
                conditions_raw = vacancy_table.find('p', string="Условия").parent.parent.find_all('p')[1:]
                conditions = ''
                for el in conditions_raw:
                    el = el.text
                    if el[0].isalpha() or el[0].isdigit():
                        conditions = conditions + str(el).replace(';', '') + ", "
                    else:
                        conditions = conditions + str(el[1:]).strip().replace(';', '') + ", "
                vacancy['conditions'] = conditions[:-2]
                property_parsed['conditions'] = True
            except Exception as e:
                vacancy['conditions'] = ""
                error_message = f"Information about conditions not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Обязанности
            try:
                responsibilities_raw = vacancy_table.find('p', string="Обязанности").parent.parent.find_all('p')[1:]
                responsibilities = ''
                for el in responsibilities_raw:
                    el = el.text
                    if el[0].isalpha() or el[0].isdigit():
                        responsibilities = responsibilities + str(el).replace(';', '') + ", "
                    else:
                        responsibilities = responsibilities + str(el[1:]).strip().replace(';', '') + ", "
                vacancy['responsibilities'] = responsibilities[:-2]
                property_parsed['responsibilities'] = True
            except Exception as e:
                vacancy['responsibilities'] = ""
                error_message = f"Information about responsibilities not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Образование
            try:
                vacancy['education'] = vacancy_table.find('p', string="Образование").parent.parent.find_all('p')[
                    -1].text
                property_parsed['education'] = True
            except Exception as e:
                vacancy['education'] = ""
                error_message = f"Information about education not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Опыт работы
            try:
                vacancy['experience'] = vacancy_table.find('p', string="Опыт работы").parent.parent.find_all('p')[
                    -1].text
                property_parsed['experience'] = True
            except Exception as e:
                vacancy['experience'] = ""
                error_message = f"Information about experience not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Требования к кандидату
            try:
                requirements_raw = vacancy_table.find('p', string="Требования").parent.parent.find_all('p')[1:]
                requirements = ''
                for el in requirements_raw:
                    el = el.text
                    if el[0].isalpha() or el[0].isdigit():
                        requirements = requirements + str(el).replace(';', '') + ", "
                    else:
                        requirements = requirements + str(el[1:]).strip().replace(';', '') + ", "
                vacancy['requirements'] = requirements[:-2]
                property_parsed['requirements'] = True
            except Exception as e:
                vacancy['requirements'] = ""
                error_message = f"Information about requirements not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Время размещения вакансии
            try:
                placement_raw = soup.find('p', class_='small').text
                placement_raw = placement_raw[placement_raw.find('·') + 1: placement_raw.rfind('·')].strip()
                placement_raw = placement_raw.split()
                vacancy[
                    'placement'] = f'{placement_raw[0]}.{month_dict[placement_raw[1].lower()]}.{placement_raw[2].replace(",", "")} {placement_raw[3]}:00'
                property_parsed['placement'] = True
            except Exception as e:
                vacancy['placement'] = ""
                error_message = f"Information about placement not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Количество просмотров
            try:
                job_views_value_raw = soup.find('p', class_='small').text
                job_views_value_raw = job_views_value_raw[job_views_value_raw.rfind('·'): -1]
                vacancy['job_views_value'] = "".join(c for c in job_views_value_raw if c.isdecimal())
                property_parsed['job_views_value'] = True
            except Exception as e:
                vacancy['job_views_value'] = ""
                error_message = f"Information about job_views_value not found: {href}. Error details: {e}"
                logging.error(error_message)

            # Укороченное описание вакансии
            vacancy['job_simple_dis_sec'] = ""
            parsed_data.append(vacancy)

    #         print(f'vacansy: {len(parsed_data)} parsed\r', end='')
    df = pd.DataFrame(parsed_data)

    # Теперь вы можете сохранять файл в этой директории
    df.to_csv(f'{output_directory}/joblab_hrefs_{datetime_to_filename}.csv', sep='|', encoding='utf-8-sig', mode='w',
              header=vacancies[0].keys(), index=False)

    df.to_csv(f'./parsers/joblab_parser/joblab_data_{datetime_to_filename}.csv', sep='|', encoding='utf-8-sig')
    logging.info('Parsing done')

except Exception as e:
    logging.error(e)


for property_name in properties:
    if property_parsed[property_name]:
        logging.info(f"{property_name.capitalize()} was successfully extracted without any code changes.")
    else:
        logging.warning(f"Possible page code change: {property_name} not found on any page.")

