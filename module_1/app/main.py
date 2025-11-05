"Скрипты запускающие работчу consumer'а или producer'а."

import logging
import os

from services.consumer import BatchConsumerApp, SingleMessageConsumerApp  # type: ignore
from services.producer import ProducerApp  # type: ignore


def consumer(group_id: str, subscribe: str, batch: bool):
    """Скрипт запуска consumer'ап.

    Parameters
    ----------
    group_id : str
        Consumer Group ID
    subscribe : str
        Топики на которые необходимо подписаться в формате строки
        разделенной запятыми
    batch : bool
        Признак батчевой обработки
    """

    # Настраиваем логирование
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s - %(name)s - %(levelname)s] - %(message)s",
    )
    logger = logging.getLogger("consumer_main")

    # Конфигурация consumer'а
    config: dict[str, str | bool] = {
        # Хостнеймы и порты серверов kafka в кластере, получем из env контейнера
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # ID группы consumer'ов, получаем из cli
        "group.id": group_id,
        # Параметр сброса смещения при возникновении ошибок, получем из env контейнера
        "auto.offset.reset": os.environ["KAFKA_CONSUMER_AUTO_OFFSET_RESET"],
        # Параметр автоматического коммита оффсета: True для последовательной обработки, False для батчевой
        "enable.auto.commit": not batch,
        # Максимальный интервал между опросами Kafka в миллисекундах, получем из env контейнера
        "max.poll.interval.ms": os.environ["KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS"],
        # Таймаут сесси, при его достижении worker убирается из группы consumer'ов и запускает ребалансировка, получем из env контейнера
        "session.timeout.ms": os.environ["KAFKA_CONSUMER_SESSION_TIMEOUT_MS"],
        # Таймаут батча, при его достижении, если получено хотя бы 1 сообщение, возвращает обработанный батч
        "batch_time_out": os.environ["KAFKA_CONSUMER_BATCH_TIMEOUT"]
    }

    # Выбираем тип обработки в зависимости от параметра batch
    consumers: dict[bool, type[SingleMessageConsumerApp | BatchConsumerApp]] = {
        False: SingleMessageConsumerApp,
        True: BatchConsumerApp,
    }

    # Создаем приложение consumer
    consumer_app = consumers[batch](config)

    # Подписываемся на топики
    consumer_app.subscribe(subscribe.split(","))  # Список топиков

    try:
        # Запускаем consumer
        consumer_app.run()

    # Обрабатываем прерывание работы consumer'а пользователем
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    # Неожиданные ошибки пишем в лог
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    # В любом случае закрываем подключение consumer'а к Kafka
    finally:
        consumer_app.close()


def producer(topic: str, key: str | None, value: str, dataclass: str | None) -> None:
    """Скрипт запуска producer'а.

    Parameters
    ----------
    topic : str
        Топик в который неоюходимо записать сообщение
    key : str | None
        Опциональный ключ сообщения
    value : str
        Значение сообщения в виде строки
    dataclass : str | None
        Опчиональный dataclass которым сериализуется сообщение
    """

    # Настраиваем логирование
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s - %(name)s - %(levelname)s] - %(message)s",
    )
    logger = logging.getLogger("producer_main")

    # Конфигурация producer'а
    config: dict[str, str | int] = {
        # Хостнеймы и порты серверов kafka в кластере, получем из env контейнера
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # От скольки реплик producer будет ждать подтверждение получения сообщения, получем из env контейнера
        "acks": os.environ["KAKFA_PRODUCER_ACKS"],
        # Сколько раз producer будет пытаться отправить сообщение повторно в случае ошибки, получем из env контейнера
        "retries": os.environ["KAFKA_PRODUCER_RETRIES"],
        # Максимально кол-во неподтвержденных сообщение в рамках 1 соединения 
        "max.in.flight.requests.per.connection": os.environ["KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION"]
    }

    # Заводим переменную для хранения заголовков
    headers: dict[str, str | None] = {"dataclass": dataclass}

    # Создаем приложение producer
    producer_app: ProducerApp = ProducerApp(config)

    try:
        # Пробем отправить сообщение и дождаться отправки
        producer_app.produce(topic, key, value, headers)
        producer_app.flush()
    # Неожиданные ошибки пишем в лог
    except Exception as e:
        logger.error(f"Unexpected message sending error: {e}")
