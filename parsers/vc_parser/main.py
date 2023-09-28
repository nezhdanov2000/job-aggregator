from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
import time
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
# selenium-stealth ? if you need

# !!!!!!!!->  pip install selenium, fake-useragent, lxml, beautifulsoup4, pandas, webdriver-manager

#options
options = webdriver.ChromeOptions()
useragent = UserAgent()
options.add_argument(f'user-agent={useragent.random}')

#headles mode
options.add_argument("--headless")

#disable webdriver
options.add_argument("--disable-extensions")
options.add_argument("--disable-extensions-file-access-check")
options.add_argument("--disable-extensions-http-throttling")
options.add_argument("--disable-infobars")
options.add_argument("--disable-web-security")
options.add_argument("--disable-blink-features=AutomationControlled")

driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))

url = 'https://vc.ru/job'
parse_datetime = datetime.now()
datetime_to_filename = f"{parse_datetime.strftime('%d-%m-%Y_%H-%M-%S')}"

# test webdriver
# url = 'https://www.whatismybrowser.com/detect/what-is-my-user-agent/'
# url = 'https://intoli.com/blog/not-possible-to-block-chrome-headless/chrome-headless-test.html'

try:
    driver.get(url=url)
    time.sleep(2)
    
    def scrollDownPage(pageHeight=0):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        newPageHeight = driver.execute_script('return document.body.scrollHeight')
        if newPageHeight > pageHeight:
            scrollDownPage(newPageHeight)
    
    scrollDownPage()
    

    with open(f'./parsers/vc_parser/vc_html_{datetime_to_filename}.html', 'w+', encoding="utf-8") as f:
        f.write(driver.page_source)
            
except Exception as e:
    print(e)
finally:
    driver.close()
    driver.quit()

# ///////////////////////////////////////


try:
    html_file = open(f'./parsers/vc_parser/vc_html_{datetime_to_filename}.html', 'r', encoding="utf-8")
    html_doc = html_file.read()
    soup = BeautifulSoup(html_doc, 'html.parser')

    parsed_data = []
    for el in soup.find_all("div", class_="feed__item"):
        data_row = {}
        data_row['vacancy_href'] = el.find("a", class_="content-header__item")['href']
        data_row['vacancy_href_parse_date'] = parse_datetime.strftime("%d/%m/%Y %H:%M:%S")
        parsed_data.append(data_row)

    with open(f'./parsers/vc_parser/vc_hrefs_{datetime_to_filename}.csv', 'w') as f:
        f.write(','.join(parsed_data[0].keys()))
        f.write('\n')
        for data_row in parsed_data:
            f.write(','.join(str(x) for x in data_row.values()))
            f.write('\n')
            
except Exception as e:
    print(e)
finally:
    html_file.close()

# ///////////////////////////////////////


def clean_text(text):
    # Удалить все символы, кроме букв, цифр, пробелов и запятых
#     return re.sub(r'[^\w\s,а-яА-ЯёЁ]', '', text)
    return text

try:
    df = pd.read_csv(f'./parsers/vc_parser/vc_hrefs_{datetime_to_filename}.csv')
    parsed_data = []
    
    for href in df['vacancy_href']:
#         time.sleep(1)
        
        headers = {
            'authority': 'vc.ru',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'max-age=0',     
            'dnt': '1',
            'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': useragent.random,
        }

        response = requests.get(
            href,
            headers=headers,
        )
        
        if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                vacancy = {}

                # Наименование работодателя
                employer_sec = soup.find(
                    'div', class_="content-header-author__name")
                employer = employer_sec.get_text(
                    strip=True) if employer_sec else "Не указано"

                # Наименование вакансии
                job_title_sec = soup.find('h1', class_="content-title")
                job_title = job_title_sec.get_text(
                    strip=True) if job_title_sec else "Не указано"

                # Укороченное описание вакансии
                job_simple_dis_sec = soup.find(
                    'div', class_="subsite_card_simple__description l-mt-2")
                job_simple_dis = job_simple_dis_sec.get_text(
                    strip=True) if job_simple_dis_sec else "Не указано"

                # Количество просмотров
                job_views_value_sec = soup.find('div', class_="views")
                job_views_value = job_views_value_sec.find('span').get_text(
                    strip=True) if job_views_value_sec else "Не указано"

                # Время размещения вакансии
                placement_sec = soup.find('time')
                job_placement_data = placement_sec['title'] if placement_sec else "Не указано"

                # Требования к кандидату
                requirements_sec = soup.find_all('h2')[0].find_next('ul')
                job_requirements = [li.get_text(strip=True) for li in
                                    requirements_sec.find_all('li')] if requirements_sec else []

                # Извлечение требований (задач)
                tasks_sec = soup.find_all('h2')[1].find_next('ul')
                job_task = [li.get_text(strip=True) for li in tasks_sec.find_all(
                    'li')] if tasks_sec else []

                # Извлечение условий работы к кандидату
                conditions_sec = soup.find_all('h2')[2].find_next('ul')
                job_conditions = [li.get_text(strip=True) for li in conditions_sec.find_all(
                    'li')] if conditions_sec else []

                # Детали вакансии
                vacancy_det_sec = soup.find(
                    'div', class_='l-island-a').find_next('p', class_='vacancy_details')
                if vacancy_det_sec:
                    # Город
                    job_city = vacancy_det_sec.find('a', href=True).get_text(strip=True,
                                                                             separator=' ') if vacancy_det_sec.find('a',
                                                                                                                    href=True) else "Город не указан"

                    # Зарплата
                    job_salary = vacancy_det_sec.find_all('span')[2].get_text(strip=True, separator=' ') if len(
                        vacancy_det_sec.find_all('span')) > 2 else "Зарплата не указана"

                    # Занятость
                    job_employment = vacancy_det_sec.find_all('span')[4].get_text(strip=True, separator=' ') if len(
                        vacancy_det_sec.find_all('span')) > 4 else "Занятость не указана"
                    
                vacancy['job_source'] = href
                vacancy['employer'] = clean_text(employer)
                vacancy['job_title'] = clean_text(job_title)
                vacancy['job_simple_dis'] = clean_text(job_simple_dis)
                vacancy['job_views_value'] = clean_text(job_views_value)
                vacancy['job_placement_data'] = clean_text(job_placement_data)
                vacancy['job_requirements'] = clean_text(",".join(job_requirements))
                vacancy['job_task'] = clean_text(",".join(job_task))
                vacancy['job_conditions'] = clean_text(",".join(job_conditions))
                vacancy['job_city'] = clean_text(job_city)
                vacancy['job_salary'] = clean_text(job_salary)
                vacancy['job_employment'] = clean_text(job_employment)

                parsed_data.append(vacancy)

    df = pd.DataFrame(parsed_data)

    df.to_csv(f'./parsers/vc_parser/vc_data_{datetime_to_filename}.csv', sep='|', encoding='utf-8-sig')
        
except Exception as e:
    print(e)
finally:
    print('end')