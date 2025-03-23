import yaml

# Путь к файлу
yaml_file = "article_data.yaml"

# Счетчик текстов длиннее 1200 слов
long_texts_count = 0
threshold = 1200

# Чтение YAML-файла
with open(yaml_file, "r", encoding="utf-8") as file:
    articles_yaml = list(yaml.safe_load_all(file))

# Обработка каждого документа
for article_list in articles_yaml:
    if article_list is None:  # Пропускаем пустые документы
        continue
    for article in article_list:
        # Извлекаем текст
        text = article["text"]
        # Считаем количество слов (разделяем по пробелам)
        word_count = len(text.split())
        # Проверяем, превышает ли текст 1200 слов
        if word_count > threshold:
            long_texts_count += 1
            print(f"Article '{article['title']}' (URL: {article['url']}) has {word_count} words")

# Итоговый результат
print(f"\nTotal number of texts longer than {threshold} words: {long_texts_count}")