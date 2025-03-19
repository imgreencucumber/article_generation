import time
import signal
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import yaml
import argparse
from datetime import datetime, timedelta
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from concurrent.futures import ThreadPoolExecutor, as_completed

stop_parsing = False

def signal_handler(sig, frame):
    global stop_parsing
    print("\nПолучен сигнал прерывания. Завершаем...")
    stop_parsing = True

signal.signal(signal.SIGINT, signal_handler)

def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-images')
    options.add_argument('--disable-webgl')
    options.add_argument('--enable-unsafe-swiftshader')
    options.add_argument('--ignore-certificate-errors')  # Игнорируем SSL-ошибки
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def parse_russian_date(date_str):
    months = {'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
              'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12}
    current_date = datetime.now().date()
    date_str = date_str.lower().strip()
    if 'дня назад' in date_str or 'дней назад' in date_str:
        return int(date_str.split()[0])
    elif 'вчера' in date_str:
        return 1
    elif 'сегодня' in date_str:
        return 0
    else:
        parts = date_str.split()
        if len(parts) == 3:
            day, month_str, year = parts
            day = int(day)
            month = months[month_str]
            year = int(year)
            article_date = datetime(year, month, day).date()
        elif len(parts) == 2:
            day, month_str = parts
            day = int(day)
            month = months[month_str]
            year = current_date.year
            article_date = datetime(year, month, day).date()
            if article_date > current_date:
                article_date = datetime(year - 1, month, day).date()
        else:
            raise ValueError(f"Неподдерживаемый формат даты: {date_str}")
        return (current_date - article_date).days

def parse_views(views_str):
    views_str = views_str.replace('\xa0', '').replace('тыс', '000').replace(',', '.')
    if '000' in views_str:
        return int(float(views_str.replace('000', '')) * 1000)
    return int(views_str)

def extract_article_text(driver):
    try:
        article_body = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="article-body"]'))
        )
        elements = article_body.find_elements(By.XPATH, ".//*[self::p or self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6 or self::ul or self::ol or self::li]")
        article_text = []
        processed_lists = set()  # Для отслеживания обработанных списков
        for elem in elements:
            tag = elem.tag_name
            text = elem.text.strip()
            if text:
                if tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    article_text.append(text)
                elif tag == 'p':
                    article_text.append(text)
                elif tag in ['ul', 'ol']:
                    # Проверяем, не обрабатывали ли мы этот список
                    list_id = elem.get_attribute('outerHTML')[:50]  # Уникальный идентификатор
                    if list_id not in processed_lists:
                        items = [li.text.strip() for li in elem.find_elements(By.TAG_NAME, 'li') if li.text.strip()]
                        if items:
                            # article_text.append(f"{tag.upper()}:")
                            article_text.extend([f"  - {item}" for item in items])
                            processed_lists.add(list_id)
                elif tag == 'li' and (not article_text or not article_text[-1].startswith("  -")):
                    article_text.append(f"  - {text}")
        return "\n".join(article_text)
    except Exception as e:
        return f"Ошибка извлечения текста: {e}"

def parse_article(article_url):
    if stop_parsing:
        return None
    driver = create_driver()
    try:
        driver.get(article_url)
        title = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'h1[data-testid="article-title"]'))
        ).text
        date_str = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'content--article-info-block__longFormat-xq'))
        ).text
        days_ago = parse_russian_date(date_str)
        views_str = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'content--article-info-block__viewsInfo-1g'))
        ).text
        views = parse_views(views_str)
        try:
            theme_elem = driver.find_element(By.CLASS_NAME, 'content--trap-ray__title-Ax')
            theme = theme_elem.text.strip()
            if not theme:
                theme = driver.execute_script("return arguments[0].innerText || arguments[0].textContent;", theme_elem).strip()
            if not theme:
                theme = "Не указана (пустой элемент)"
        except NoSuchElementException:
            theme = "Не указана"
        text = extract_article_text(driver)

        print(f"Отладка: {title} | Просмотры: {views} | Дней назад: {days_ago} | Тема: {theme}")
        if views > 500 and days_ago <= 180:
            article_data = {
                'title': title,
                'days_ago': days_ago,
                'views': views,
                'theme': theme,
                'text': text,
                'url': article_url
            }
            print(f"Статья добавлена: {title}")
            return article_data
        else:
            print(f"Статья отклонена: {title} (Просмотры: {views}, Дней назад: {days_ago})")
            return None
    except Exception as e:
        print(f"Ошибка при парсинге {article_url}: {e}")
        return None
    finally:
        driver.quit()

def append_batch_to_yaml(batch, filename="test.yaml"):
    with open(filename, "a", encoding="utf-8") as f:
        try:
            yaml.dump(batch, f, allow_unicode=True, default_flow_style=False, Dumper=yaml.SafeDumper, default_style='|')
            f.write("---\n")
        except Exception as e:
            print(f"Ошибка записи в YAML: {e}")
            print("Проблемные данные:", batch)

def load_urls_from_file(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Ошибка: файл {filename} не найден.")
        sys.exit(1)

def main(max_links=5000, max_workers=8, links_file=None):  # Увеличено до 8 потоков
    global stop_parsing

    if links_file:
        article_urls = load_urls_from_file(links_file)
        print(f"Загружено {len(article_urls)} ссылок из файла {links_file}")
    else:
        driver = create_driver()
        processed_urls = set()
        all_urls = set()
        try:
            url = "https://dzen.ru/articles"
            print(f"Открываю {url}")
            driver.get(url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "desktop2--card-article__cardLink-16"))
            )
            while len(processed_urls | all_urls) < max_links and not stop_parsing:
                new_urls = scroll_to_load_all(driver, max_links, processed_urls)
                all_urls.update(new_urls)
                with open("processed_urls.txt", "a", encoding="utf-8") as f:
                    for url in new_urls:
                        f.write(f"{url}\n")
                processed_urls = set(load_urls_from_file("processed_urls.txt"))
                print(f"Всего уникальных ссылок: {len(processed_urls)} из {max_links}")
                if len(new_urls) == 0:
                    print("Новых ссылок не найдено, завершаем сбор.")
                    break
                driver.get(url)
            article_urls = list(all_urls)
            print(f"\nНайдено {len(article_urls)} ссылок для обработки")
        except TimeoutException:
            print("Ошибка: страница не загрузилась вовремя.")
            article_urls = list(all_urls)
        finally:
            driver.quit()

    # Обработка страниц
    open("test_data.yaml", "w").close()  # Очистка файла перед началом
    saved_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(parse_article, url): url for url in article_urls}
        for future in as_completed(future_to_url):
            if stop_parsing:
                executor.shutdown(wait=False)
                break
            article_data = future.result()
            if article_data:
                append_batch_to_yaml([article_data])  # Сохраняем сразу каждую статью
                saved_count += 1
                print(f"Сохранено {saved_count} статей")

    print(f"Итого сохранено {saved_count} статей в articles_data.yaml")
    print("Парсер завершил работу.")

def scroll_to_load_all(driver, max_links, processed_urls, max_scrolls=50):
    print("Начало прокрутки главной страницы...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    links_collected = 0
    scroll_count = 0
    new_urls = set()

    while scroll_count < max_scrolls and len(processed_urls | new_urls) < max_links:
        if stop_parsing:
            break
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        links = driver.find_elements(By.CLASS_NAME, "desktop2--card-article__cardLink-16")
        for link in links:
            href = link.get_attribute("href")
            if href and "dzen.ru/a/" in href and href not in processed_urls:
                new_urls.add(href)
        links_collected = len(new_urls)
        print(f"\rПрокрутка {scroll_count + 1}/{max_scrolls}: собрано {links_collected} новых ссылок (всего {len(processed_urls | new_urls)})", end="", flush=True)
        
        if scroll_count % 10 == 0 and scroll_count > 0:
            driver.execute_script("document.querySelectorAll('.desktop2--card-article__cardLink-16').forEach(el => el.remove());")
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        scroll_count += 1
    
    print(f"\nПрокрутка завершена: собрано {links_collected} новых ссылок.")
    return new_urls

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Парсер статей с dzen.ru")
    parser.add_argument("--max-links", type=int, default=5000, help="Максимальное количество уникальных ссылок")
    parser.add_argument("--max-workers", type=int, default=8, help="Количество параллельных потоков")
    parser.add_argument("--links-file", type=str, help="Путь к файлу со списком ссылок")
    args = parser.parse_args()
    main(args.max_links, args.max_workers, args.links_file)