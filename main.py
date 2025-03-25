import os
import psycopg2
from yandex_cloud_ml_sdk import YCloudML
from qdrant_client import QdrantClient

# Параметры подключения
POSTGRES_PARAMS = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": 5432,
    "database": "dzen_db",
    "user": "postgres",
    "password": "1"
}
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = 6333
YANDEX_FOLDER_ID = "b1gg1kh7b3mtup23kjbd"
YANDEX_AUTH = "AQVNwg5-e736XnSpAvHuiVvIAJJQlBoqb0t1D1Jb"

def truncate_text(text, max_words=1200):
    words = text.split()
    if len(words) > max_words:
        truncated = " ".join(words[:max_words])
        print(f"Text truncated from {len(words)} to {max_words} words")
        return truncated
    return text

def get_embedding(text, sdk):
    doc_model = sdk.models.text_embeddings("doc")
    truncated_text = truncate_text(text)
    result = doc_model.run(truncated_text)
    return list(result.embedding)

def get_closest_articles(embedding, qdrant_client, collection_name="articles", limit=2):
    search_result = qdrant_client.query_points(
        collection_name=collection_name,
        query=embedding,
        limit=limit
    ).points
    return [hit.id for hit in search_result]

def get_article_text(article_id, cursor):
    cursor.execute("SELECT text FROM articles WHERE id = %s", (article_id,))
    result = cursor.fetchone()
    return result[0] if result else None

def generate_article(news_text, similar_texts, sdk):
    messages = [
        {
            "role": "system",
            "text": "Ты — профессиональный журналист. Напиши новую статью, комбинируя информацию из предоставленной новости и двух похожих статей. Сохраняй стиль и тон оригиналов, добавляй свои детали для связности и уникальности. Результат должен быть логичным, интересным и не повторять дословно исходные тексты. Помни, что твоя задача — не просто склеить тексты, а создать новое произведение. Укажи места для вставки изображения."
        },
        {
            "role": "user",
            "text": f"Новая новость:\n{news_text}\n\nПохожая статья 1:\n{similar_texts[0]}\n\nПохожая статья 2:\n{similar_texts[1]}"
        }
    ]
    completion_model = sdk.models.completions("yandexgpt").configure(temperature=0.5)
    result = completion_model.run(messages)
    return next(iter(result), "Ошибка генерации статьи")

def main():
    # Инициализация клиентов
    sdk = YCloudML(folder_id=YANDEX_FOLDER_ID, auth=YANDEX_AUTH)
    qdrant_client = QdrantClient(QDRANT_HOST, port=QDRANT_PORT, timeout=30)
    conn = psycopg2.connect(**POSTGRES_PARAMS)
    cursor = conn.cursor()

    print("Введите текст новости (для завершения ввода нажмите Enter дважды):")
    news_lines = []
    while True:
        line = input()
        if line == "":
            if news_lines and news_lines[-1] == "":
                break
            news_lines.append("")
        else:
            news_lines.append(line)
    news_text = "\n".join(news_lines).strip()

    if not news_text:
        print("Ошибка: текст новости не введен.")
        cursor.close()
        conn.close()
        return

    # Вычисление эмбеддинга новости
    print("Computing embedding for input news...")
    news_embedding = get_embedding(news_text, sdk)

    # Поиск ближайших статей в Qdrant
    print("Searching for similar articles in Qdrant...")
    similar_article_ids = get_closest_articles(news_embedding, qdrant_client)
    if len(similar_article_ids) < 2:
        print("Warning: Found fewer than 2 similar articles.")
        print(f"Similar articles found: {similar_article_ids}")
    else:
        print(f"Found similar articles: {similar_article_ids}")

    # Извлечение текстов похожих статей из PostgreSQL
    similar_texts = []
    for article_id in similar_article_ids:
        text = get_article_text(article_id, cursor)
        if text:
            similar_texts.append(text)
        else:
            print(f"Article ID {article_id} not found in PostgreSQL.")

    # Генерация новой статьи
    print("Generating new article...")
    if len(similar_texts) == 2:
        generated_article = generate_article(news_text, similar_texts, sdk)
    else:
        generated_article = "Ошибка: недостаточно похожих статей для генерации."

    print("\nСгенерированная статья:")
    print("-" * 50)
    print(generated_article.text)
    print("-" * 50)

    # Закрытие соединений
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()