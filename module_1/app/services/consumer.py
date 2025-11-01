"""Классы kafka-consumer."""

import logging
import signal

from confluent_kafka import Consumer, KafkaError, KafkaException, Message  # type: ignore
from dataclasses_avroschema import AvroModel

import serde.messages as messages  # type: ignore


class SingleMessageConsumerApp:
    """Kafka-consumer читающий сообщения по одному."""

    # Констант таймаута опроса новых сообщений из Kafka
    DEFAULT_POLL_TIMEOUT: float = 1.0

    def __init__(self, config: dict[str, str | bool]) -> None:
        """Создает новый экземпляр SingleMessageConsumer

        Parameters
        ----------
        config : dict[str, str  |  bool]
            Конфигурация kafka-consumer в виде dict'а
        """

        # Конфигурация consumer'а
        self.consumer: Consumer = Consumer(config)
        self.running = False

        # Настраиваем логирование
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s - %(name)s - %(levelname)s] - %(message)s",
        )
        self.logger = logging.getLogger(self.__class__.__name__)

        # Обработка сигналов для graceful остановки
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum: int, frame: object) -> None:
        """Обработчик сигналов для прерывания главного цикла.

        Parameters
        ----------
        signum : int
            Тип сигнала
        frame : object
            Stack frame, опеределенг для совместимости
        """
        self.logger.info(f"Recived signal {signum}. Shutting down...")
        self.running = False

    def subscribe(self, topics: list[str]) -> None:
        """Подписка на топики

        Parameters
        ----------
        topics : list[str]
            Список топиков на которые необходимо подписатся consumer'у.
        """
        self.consumer.subscribe(topics)
        self.logger.info(f"Subscribed to: {topics}")

    def poll_messages(self, timeout: float = DEFAULT_POLL_TIMEOUT) -> Message | None:
        """Получает новые сообщения из топиков Kafka.

        Parameters
        ----------
        timeout : float, optional
            Таймаут опроса, по умолчанию DEFAULT_POLL_TIMEOUT

        Returns
        -------
        Message
            Новое полученное сообщение
        None
            Если нет новых сообщений

        Raises
        ------
        KafkaException
            Ошибка из Kafka
        """
        try:
            # Получение сообщения
            msg = self.consumer.poll(timeout)

            # Если новых сообщений нет, возвращаем None
            if msg is None:
                return None

            # Если в сообщении содержится ошибка райзим
            if msg.error():
                # Особая обработка окончания партиции
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.debug(f"Partition end reached {msg.topic()} [{msg.partition()}]")
                    return None
                # Райзми ошибку в виде KafkaException
                else:
                    raise KafkaException(msg.error())

            # Возвращаем полученное сообщение
            else:
                return msg

        # Если из Kafka пришла ошибка - пишем в лог
        except KafkaException as e:
            self.logger.error(f"Error recived from Kafka: {e}")
            return None

        # Если в ходе опроса возникла ошибка - пишем в лог
        except Exception as e:
            self.logger.error(f"Message polling error: {e}")
            return None

    def process_message(self, msg: Message) -> None:
        """Обработка сообщения. Здесь может быть реализована логика обработки сообщения,
        но мы просто пишем в лог

        Parameters
        ----------
        msg : Message
            Сообщение полученное из Kafka
        """
        try:
            # Декодирование ключа и значения
            key, value = self.deserialize_message(msg)

            # Запись в лог
            self.logger.info(
                f"Message recived: "
                f"topic={msg.topic()}, "
                f"partition={msg.partition()}, "
                f"offset={msg.offset()}, "
                f"key={key}, "
                f"value={value}",
            )

        # Ошибка обработки сообщеня пишется в лог
        except Exception as e:
            self.logger.error(f"Message processing exception: {e}")

    def deserialize_message(self, msg: Message) -> tuple[str | None, object]:
        """Десериализует полученное сообщение.

        Parameters
        ----------
        msg : Message
            Сообщение полученное из Kafka

        Returns
        -------
        tuple[str | None, object]
            Десериализованное сообщение в виде котрежа ключ, значение
        """

        # Приводим заголовки сообщения к dict'у
        headers: dict[str, str] = {}
        if msg.headers():
            headers = self.get_schema_from_headers(msg.headers())

        try:
            # Десериализуем ключь при его наличии в строку
            key: str | None = msg.key().decode("utf-8") if msg.key() else None
            value: object

            # Если в заголовках есть dataclass десериализуем в него
            if "dataclass" in headers:
                message_class: AvroModel = getattr(messages, headers["dataclass"])
                value = message_class.deserialize(msg.value())
            # Если в заголовках нет dataclass'а десериализуем в строку
            else:
                value = msg.value().decode("utf-8")

        # Если в ходе десериализации не удалось получить атрибут dataclass из messages
        # пишем в лог и возвращаем сериализованные ключ и значение
        except AttributeError as e:
            self.logger.error(f"Unknown `dataclass` {headers['dataclass']}. {e}")
            return msg.key(), msg.value()

        # В ходе десериализации возникла неожиданная ошибка пишем в лог и возвращаем
        # сериализованные ключ и значение
        except Exception as e:
            self.logger.error(
                f"Unexpected deserialization error: {e} "
                f"Message {msg.topic()} [{msg.partition()}][{msg.offset()}]"
            )
            return msg.key(), msg.value()

        return key, value

    def get_schema_from_headers(self, headers: list[tuple[str, bytes]]) -> dict[str, str]:
        """Приводит headers к dict'у

        Parameters
        ----------
        headers: list[tuple[str, bytes]]
            Заголовки полученные с сообщением из Kafak

        Returns
        -------
        dict[str, str]
            Заголовки в виде dict'а
        """
        return {key: value.decode("utf-8") for key, value in headers}

    def run(self) -> None:
        """Основной цикл consumer'а"""
        self.running = True
        self.logger.info("Kafka consumer running...")

        try:
            # Цикл выполняется пока поле running == True
            while self.running:
                # Получаем сообщение из метода poll_messages
                msg = self.poll_messages()

                # Если сообщение не пустое передаем его в метода process_message
                if msg is not None:
                    self.process_message(msg)

        # Неожиданные ошибки пишем в лог
        except Exception as e:
            self.logger.error(f"Error in main loop: {e}")

        # Всегда закрываем подключение consumer'а к Kafka
        finally:
            self.close()

    def close(self) -> None:
        """Закрывает подключение consumer'а к Kafka"""

        self.logger.info("Consumer closing...")
        self.consumer.close()
        self.logger.info("Consumer closed")


class BatchConsumerApp(SingleMessageConsumerApp):
    """Kafka-consumer читающий сообщения батчами по 10 штук."""

    # Констант таймаута опроса новых сообщений из Kafka
    DEFAULT_POLL_TIMEOUT: float = 1.0
    # Константа размера батча
    BATCH_SIZE: int = 10

    def poll_messages(self, timeout=DEFAULT_POLL_TIMEOUT) -> list[Message]:
        """Опрашивает новые сообщения из Kafka пока не будет получено кол-во сообщений
        равное self.BATCH_SIZE, после чего коммитит смещение и возвращает батч сообщений
        для обработки.

        Parameters
        ----------
        timeout : _type_, optional
            Таймаут опроса, по умолчанию DEFAULT_POLL_TIMEOUT

        Returns
        -------
        list[Message]
            Список сообщени полученных из Kafka длинны self.BATCH_SIZE

        Raises
        ------
        KafkaException
            Ошибка из Kafka
        """

        # Определяем список который будет батч сообщений
        msg_list: list[Message] = []

        # Запускаем внутренний цикл для опроса сообщений
        while self.running:
            try:
                # Получаем сообщение
                msg = self.consumer.poll(timeout)

                # Если сообщение пустое переходим к следующей итерации
                if msg is None:
                    continue

                # Если в сообщении содержится ошибка райзим
                if msg.error():
                    # Особая обработка окончания партиции
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug(
                            f"Partition end reached {msg.topic()} [{msg.partition()}]"
                        )
                    # Райзми ошибку в виде KafkaException
                    else:
                        raise KafkaException(msg.error())
                    
                # Если сообщение не пустое добавляем его к батчу
                else:
                    msg_list.append(msg)

                    # Если батч достиг длинны self.BATCH_SIZE
                    if len(msg_list) == self.BATCH_SIZE:
                        # коммитим смещение
                        self.consumer.commit(asynchronous=False)
                        # возвращаем батч
                        return msg_list

            # Если из Kafka пришла ошибка - пишем в лог и переходим 
            # к следующей итерации
            except KafkaException as e:
                self.logger.error(f"Error recived from Kafka: {e}")
                continue

            # Если в ходе опроса возникла ошибка - пишем в лог и переходим
            # к следующей итерации
            except Exception as e:
                self.logger.error(f"Message polling error: {e}")
                continue

        return msg_list

    def process_message(self, msg: list[Message]) -> None:
        """Обрабатывает сообщения в батче метоом process_message родительского класса.

        Parameters
        ----------
        msg : list[Message]
            Батч сообщений полученных из Kafka
        """
        self.logger.info(f"Recived messages batch fo len: {len(msg)}")
        for message in msg:
            super().process_message(message)
