# Yandex Practicum. Apache Kafka для разработки и архитектуры. Модуль №1. Практическая работы

## Структура модуля

```
module_1
    ├── README.md
    ├── app
    │   ├── Dockerfile
    │   ├── __init__.py
    │   ├── cli.py
    │   ├── main.py
    │   ├── requirements.txt
    │   ├── serde
    │   │   ├── __init__.py
    │   │   └── messages.py
    │   └── services
    │       ├── __init__.py
    │       ├── consumer.py
    │       └── producer.py
    ├── compose.yml
    ├── test
    │   ├── produce_dataclass.sh
    │   └── produce_simple.sh
    └── topic.txt
```

## Запуск кластера
Для запуска кластера перейдите в директорю `module_1` и выполните команду:

```bash
docker compose up -d --scale app=<количество желаймых реплик приложения>
```

Все реплики приложения будут иметь 