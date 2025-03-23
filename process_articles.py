import yaml
import psycopg2
from yandex_cloud_ml_sdk import YCloudML
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
import time
from yandex_cloud_ml_sdk._exceptions import AioRpcError

# Параметры подключения к PostgreSQL
conn_params = {
    "host": "localhost",
    "port": 5432,
    "database": "dzen_db",
    "user": "postgres",
    "password": "1"
}

# 1. Загрузка данных в PostgreSQL
with open("article_data.yaml", "r", encoding="utf-8") as file:
    articles_yaml = list(yaml.safe_load_all(file))

conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

insert_query = """
INSERT INTO articles (days_ago, text, theme, title, url, views)
VALUES (%s, %s, %s, %s, %s, %s)
RETURNING id;
"""

article_ids = []
for article_list in articles_yaml:
    if article_list is None:
        continue
    for article in article_list:
        cursor.execute(insert_query, (
            article["days_ago"],
            article["text"],
            article["theme"],
            article["title"],
            article["url"],
            article["views"]
        ))
        article_id = cursor.fetchone()[0]
        article_ids.append((article_id, article["text"]))
        print(f"Inserted article with ID: {article_id}")

conn.commit()

# 2. Вычисление эмбеддингов с Yandex
sdk = YCloudML(
    folder_id="b1gg1kh7b3mtup23kjbd",
    auth="AQVNwg5-e736XnSpAvHuiVvIAJJQlBoqb0t1D1Jb",
)
doc_model = sdk.models.text_embeddings("doc")

# Функция для обрезки текста до ~2000 токенов
def truncate_text(text, max_words=1200):
    words = text.split()
    if len(words) > max_words:
        truncated = " ".join(words[:max_words])
        print(f"Text truncated from {len(words)} to {max_words} words")
        return truncated
    return text

embeddings = []
for article_id, text in article_ids:
    truncated_text = truncate_text(text)
    try:
        embedding = doc_model.run(truncated_text)
        embeddings.append((article_id, embedding))
        print(f"Computed embedding for article ID: {article_id}")
    except AioRpcError as e:
        if "number of input tokens must be no more than" in str(e):
            print(f"Skipping article ID {article_id}: too many tokens after truncation ({e})")
        else:
            raise  # Если ошибка не связана с токенами, прерываем выполнение
    # time.sleep(0.5)

# 3. Загрузка в Qdrant
qdrant_client = QdrantClient("localhost", port=6333, timeout=30)
collection_name = "articles"
if embeddings:  # Проверяем, что есть хотя бы один эмбеддинг
    vector_size = len(embeddings[0][1])
    if not qdrant_client.collection_exists(collection_name):
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config={"size": vector_size, "distance": "Cosine"}
        )

    points = [
        PointStruct(id=article_id, vector=embedding, payload={"article_id": article_id})
        for article_id, embedding in embeddings
    ]

    qdrant_client.upsert(collection_name=collection_name, points=points)
else:
    print("No embeddings to upload to Qdrant.")

cursor.close()
conn.close()

print(f"Processed {len(points) if embeddings else 0} articles!")