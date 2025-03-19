import yaml

def count_articles_without_theme(filename="article_data.yaml"):
    no_theme_count = 0
    total_articles = 0

    try:
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
            documents = content.split("---\n")
            
            for i, doc in enumerate(documents):
                if not doc.strip():
                    continue  # Пропускаем пустые документы
                try:
                    articles = yaml.safe_load(doc)
                    if not articles or not isinstance(articles, list):
                        print(f"Пропущен документ {i}: не является списком или пустой")
                        print(f"Содержимое: {doc[:100]}...")
                        continue
                    # Берем первый элемент списка
                    article = articles[0] if articles else None
                    if article and isinstance(article, dict):
                        total_articles += 1
                        theme = article.get("theme", "").strip()
                        if theme == "Не указана":
                            no_theme_count += 1
                    else:
                        print(f"Пропущен документ {i}: не содержит словарь")
                        print(f"Содержимое: {doc[:100]}...")
                except yaml.YAMLError as e:
                    print(f"Ошибка парсинга YAML в документе {i}: {e}")
                    print(f"Проблемный документ: {doc[:100]}...")

        print(f"Всего статей: {total_articles}")
        print(f"Статей без темы ('Не указана'): {no_theme_count}")
        if total_articles > 0:
            print(f"Процент статей без темы: {no_theme_count / total_articles * 100:.2f}%")

    except FileNotFoundError:
        print(f"Ошибка: файл {filename} не найден.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    count_articles_without_theme()