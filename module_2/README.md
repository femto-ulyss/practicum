# Yandex Practicum. Apache Kafka для разработки и архитектуры. Модуль №2. Практическая работы

## Структура модуля

```
module_2
    ├── app
    │   ├── __init__.py
    │   ├── message_processor.py
    │   ├── requirements.txt
    │   ├── models
    │   │   ├── __init__.py
    │   │   ├── message.py
    |   |   └── utils.py
    │   └── utils
    │       ├── __init__.py
    │       └── censor.py
    ├── init
    |   └── kafka-init.sh
    ├── test
    |   ├── autotest.sh
    |   └── data.txt
    ├── compose.yml
    └── README.md
```

Приложение располагается в директории `app` и представляет собой модуль с **faust**-приложением.

## Запуск кластера
Для запуска кластера перейдите в директорю `module_2` и выполните команду:

```bash
docker compose up -d
```
Будет поднят кластер **Kafka** с 3мя брокерами, все необходимы топики буду созданы автоматически.

## Проврка работы приложения

1. Зарустите кластер:
```bash
docker compose up -d
```
2. Запустите прилоежени:
```bash
faust -A app.message_processor worker -l info
```
3. Запустите тестовый скрипт:
```bash
./test/autotest.sh ./test/data.txt
```
4. В ходе тестирования процесс обработки будет логироваться в логе **faust**'а
5. По завершении работы скрипта можно проверить:
   - Топик входящих сообщений: [messages](http://localhost:8080/ui/clusters/local-kafka-cluster/all-topics/messages)
   - Топик исходящий сообщений: [filtered_messages](http://localhost:8080/ui/clusters/local-kafka-cluster/all-topics/filtered_messages)
   - Заблокированных пользователей: [blocked_users](http://localhost:8080/ui/clusters/local-kafka-cluster/all-topics/blocked_users)
   - Цензурируемые слова: [blocked_words](http://localhost:8080/ui/clusters/local-kafka-cluster/all-topics/blocked_words)
   - Заблокированные сообщения: [blocked_messages](http://localhost:8080/ui/clusters/local-kafka-cluster/all-topics/blocked_messages)

>> Данные для ручно проверки можно найти в файле: `./test/data.txt`

## Используемы агенты
Для работы приложение использует три агента работающие с потоками данных из **Kafka**:

- `app.message_processor.process_message`
- `app.message_processor.block_user`
- `app.message_processor.block_word`

### `app.message_processor.process_message`

Агент обрабатывающий поток из основного входящего топика: `messages`. Проверяет блокировку со стороны получателя и маскирует цензурируемые слова в теле сообщения.

Если отправка заблокирована записывает сообщение в сервисный топик `blocked_messages`, иначе записывает сообщения в `filtered_messages`.

Если в теле сообщения присутсвуют цензурируемые слова маскирует их `*` от второго до предпоследнего символа.

#### Процесс работы

1. Агент итериуется по потоку событий из топика `messages`
2. Каждое полученное из топика событие приводится к классу `app.models.messages.Message`
3. Проверяется наличие цензурируемых слов в теле сообщения, если таковые есть они маскируются
4. Если отправка сообщений не заблокирована сообщение записывается в топик `filtered_messages`, иначе сообщение записывается в топик `blocked_messages`

### `app.message_processor.block_user`

Агент обрабатывает сообщения в топик `blocked_users`. При получени сообщения о блокировке собщение записывает в таблицу `blocked_users`.

#### Процесс работы

1. Агент итериуется по потоку событий из топика `blocked_users`
2. Каждое полученное событие приводится к классу `app.models.utils.SenderBlock`
3. По полю `SenderBlock.user` из таблицы `blocked_users` получается текущее множество заблокрованных пользователей 
4. К текущем множеству добавляется значение `SenderBlock.block`
5. Обновленное множество записывается в таблицу `blocked_users`

### `app.message_processor.block_word`

Агент обрабатывает сообщения в топик `blocked_words`. Получении сообщения о цензурировании слова оно записывается в таблицу `blocked_words`.

#### Процесс работы

1. Агент итериуется по потоку событий из топика `blocked_words`
2. Каждое полученное событие приводится к классу `app.models.utils.WordBlock`
3. Слово записывается в таблицу `blocked_words` как ключ со значение `True`


## Классы сообщений

- `app.models.messages.Message`
- `app.models.utils.SenderBlock`
- `app.models.utils.WordBlock`

### `app.models.messages.Message`

Основной класс сообщений в системе. Содержит поля:
  
  - `sender_id`: *str* - отправитель сообщения
  - `reciver_id`: *str* - получатель сообщения
  - `body`: *str* - тело сообщения

### `app.models.utils.SenderBlock`

Сообщение использующееся для блокировки полученя сообщения от определенных пользователей. Содержит поля:

- `user`: *str* - идентификато инициатора блокировки
- `block`: *str* - идентификато пользователя получение сообщений от которого будет заблокировано

### `app.models.utils.WordBlock`

Сообщение использующееся для добавления слов в цензурируемы список. Содержит поля:

- `word`: *str* - словок которое будет маскироваться в теле сообщения

