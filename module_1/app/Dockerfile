# Базовый образ для python
FROM python:3.12-slim

# Устанавливаем рабочу директорию /app в контейнере
WORKDIR /app

# Копируем список зависимостей
COPY ./requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Контейнер будет ждать внешней команды бесконечно долго
CMD sleep infinity