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

Приложение располагается в директории `app` и представляет собой контейнеризованный cli-клиент для работы с **Kafka**.

## Запуск кластера
Для запуска кластера перейдите в директорю `module_1` и выполните команду:

```bash
docker compose up -d --scale app=<количество желаймых реплик приложения от 1 до 6>
```

Все реплики приложения будут иметь имя `module_1-app-<номер реплики>`.

## Проврка работы приложения

>> В данном пример будет использовано 2 реплики контейнера с приложением. Для изменения количества репли используйте параметр: `--scale app=<кол-во реплик>`

1. Зарустите кластер:
```bash
docker compose up -d --scale app=2
```
2. Создайте топик:
```bash
docker exec -it kafka1 kafka-topics --create --topic test_topic --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --partitions 3 --replication-factor 2
```
3. Запустите `SingleMessageConsumerApp`:
```bash
docker exec -it module_1-app-1 python /app/cli.py consumer --subscribe test_topic --group_id single_consumer
docker exec -it module_1-app-2 python /app/cli.py consumer --subscribe test_topic --group_id single_consumer
```
4. Запустите `BatchConsumerApp`:
```bash
docker exec -it module_1-app-1 python /app/cli.py consumer --subscribe test_topic --group_id batch_consumer --batch
docker exec -it module_1-app-2 python /app/cli.py consumer --subscribe test_topic --group_id batch_consumer --batch
```
5. Для отправки простых сообщений запустите sh-скрит `module_1.test.produce_simple.sh`:
```bash
./module_1/test/produce_simple.sh
```
6. Для отправки сериализованных с помощью `dataclasses_avro.AvroModel` сообщений запутите sh-скрипт `module_1.test.produce_dataclass.sh`
```bash
./module_1/test/produce_dataclass.sh
```


## Используемы классы
Для работы приложение использует три класса реализующих работы с **Kafka**:

- `app.services.consumer.SingleMessageConsumerApp`
- `app.services.consumer.BatchConsumerApp`
- `app.services.producer.ProducerApp`

### `app.services.consumer.SingleMessageConsumerApp`

Класс `consumer` обрабатывающий сообщения в **Kafka** по одному.
В качестве конфигурации принимает `dict` вида:

```python
    {    
        "bootstrap.servers": str,       # Хостнеймы и порты серверов kafka в кластере, получем из env контейнера
        "group.id": str,                # ID группы consumer'ов, получаем из cli
        "auto.offset.reset": str,       # Параметр сброса смещения при возникновении ошибок, получем из env контейнера
        "enable.auto.commit": bool,     # Параметр автоматического коммита оффсета: True для последовательной обработки, False для батчевой
        "max.poll.interval.ms": int,    # Максимальный интервал между опросами Kafka в миллисекундах, получем из env контейнера
        "session.timeout.ms": int,      # Таймаут сесси, при его достижении worker убирается из группы consumer'ов и запускает ребалансировка, получем из env контейнера
    }
```
Объект класса создается скриптом `app.main.consumer` передавая в него полученный из cli и env-контейнера параметры. После того как объект создан и сконфигурирован скрипт запускает основной цикл `consumer'а` вызывая метод `run()`.

#### Процесс работы

1. В ходе основного цикла `consumer` опрашивает **Kafka** используя метод `.poll()` вложенного объекта типа `confluent_kafka.Consumer`. Если `poll()` вернул не пустое сообщение и метод `error()` сообщения возвращает `None`, `consumer` считает сообщени прочитанными, коммитик `offset` и возвращает объект сообщения в основной цикл.
2. Далее объект передается в метод `process_message()`, который десериализует сообщение с помошью метода `deserialize_message()` и отправляет результат в консоль. 

### `app.services.consumer.BatchConsumerApp`

Класс `consumer` обрабатывающий сообщения в **Kafka** батчами по 10 штук. Является наследником класса `app.services.consumer.SingleMessageConsumerApp` с переопределенными методами `poll_messages()` и `process_message()`.

В качестве конфигурации принимает `dict` вида:

```python
    {    
        "bootstrap.servers": str,       # Хостнеймы и порты серверов kafka в кластере, получем из env контейнера
        "group.id": str,                # ID группы consumer'ов, получаем из cli
        "auto.offset.reset": str,       # Параметр сброса смещения при возникновении ошибок, получем из env контейнера
        "enable.auto.commit": bool,     # Параметр автоматического коммита оффсета: True для последовательной обработки, False для батчевой
        "max.poll.interval.ms": int,    # Максимальный интервал между опросами Kafka в миллисекундах, получем из env контейнера
        "session.timeout.ms": int,      # Таймаут сесси, при его достижении worker убирается из группы consumer'ов и запускает ребалансировка, получем из env контейнера
    }
```
Объект класса создается скриптом `app.main.consumer` передавая в него полученный из cli и env-контейнера параметры. После того как объект создан и сконфигурирован скрипт запускает основной цикл `consumer'а` вызывая метод `run()`.

#### Процесс работы

1. В ходе основного цикла `consumer` опрашивает **Kafka** используя метод `.poll()` вложенного объекта типа `confluent_kafka.Consumer`. Если `poll()` вернул не пустое сообщение и метод `error()` сообщения возвращает `None`, `consumer` добавляет сообщение в список `msg_list`. Когда длинна списка достигает 10, `consumer` коммитик последний `offset` и возвращает список в основной цикл.
2. Далее объект передается в метод `process_message()`, который в цикле десериализует сообщение с помошью метода `deserialize_message()` и отправляет результат в консоль. 

### `app.services.producer.ProducerApp`

Класс `producer` отправляюзий сообщения в **Kafka**.

В качестве конфигурации принимает `dict` вида:

```python
    {
        "bootstrap.servers": str,   # Хостнеймы и порты серверов kafka в кластере, получем из env контейнера
        "acks": str,                # От скольки реплик producer будет ждать подтверждение получения сообщения, получем из env контейнера
        "retries": str,             # Сколько раз producer будет пытаться отправить сообщение повторно в случае ошибки, получем из env контейнера
    }
```
Объект класса создается скриптом `app.main.producer` передавая в него полученный из cli и env-контейнера параметры. После того как объект создан и сконфигурирован скрипт отправляет сообщение вызывая метод `produce()`.

#### Процесс работы

1. `producer` получает из скрипта `app.main.producer` сообщение для отправки. Если в загаловках сообщения передан `dataclass` сообщение сериализуется с помощью `dataclasses_avro.AvroModel`.
2. Сообщение отправляется методом `produce()` вложенного объекта `confluent_kafka.Producer`. За один раз возможна отправка только 1 сообщения.