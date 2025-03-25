FROM python:3.12.9

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY main.py .

CMD ["python", "main.py"]