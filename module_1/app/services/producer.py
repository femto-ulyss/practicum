"""Классы kafka-producer'а."""

import json
import logging

from confluent_kafka import KafkaError, Message, Producer  # type: ignore
from serde import messages  # type: ignore
from serde.messages import BaseMessage  # type: ignore


class ProducerApp:
    """Kafka-producer отправляющий сообщения в Kafka."""

    def __init__(self, config: dict[str, str | int]) -> None:
        """Создает новый экземпляр ProducerApp.

        Parameters
        ----------
        config : dict[str, str  |  int]
            Конфигурация kafka-producer'а в виде dict'а
        """

        # Конфигурация consumer'а
        self.producer: Producer = Producer(config)

        # Настраиваем логирование
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s - %(name)s - %(levelname)s] - %(message)s",
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def delivery_report(self, err: KafkaError, msg: Message) -> None:
        """Callback-функция отчета о доставкею

        Parameters:
        err: KafkaError
            Ошибка отправки сообщения
        msg: Message
            Сообщение которое было отправлено в Kafka
        """

        # Если возникла ошибка сообщения пишем в лог
        if err is not None:
            self.logger.error(f"Message delivery fauled: {err}")
        # Если сообщение отправлено тоже пишем в лог
        else:
            self.logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def serialize_message(
        self, key: str | int | None, value: str, dataclass: str | None
    ) -> tuple[bytes | None, bytes]:
        """Сериализует отправляемое сообщение как датакласс

        Parameters
        ----------
        value : str
            Значение сообщения
        dataclass : str
            Тип датакаласс для сериализации

        Returns
        -------
        bytes
            Значение сообщения серилизованное как датакласс или строка
        """
        # Задаем переменную и заранее сереализуем в нее ключ сообщения
        bytes_key: bytes | None = None
        if key:
            if isinstance(key, int):
                bytes_key = str(key).encode("utf-8")
            else:
                bytes_key = key.encode("utf-8")

        # Заранее приведем значение сообщения в байты как строку
        bytes_value: bytes = value.encode("utf-8")

        # Попробуем сериализовать значение сообщения как dataclass
        if dataclass:
            try:
                message_class: BaseMessage = getattr(messages, dataclass)(**json.loads(value))
                bytes_value = message_class.serialize()

                # Если ключ не передан используем свойство датакласса
                if not key:
                    bytes_key = str(getattr(message_class, message_class.partition_key)).encode(
                        "utf-8"
                    )
            # Если возникает ValueError - пишем в лог, отправляем сообщение без сериализации
            except ValueError as e:
                self.logger.error(f"{e}. Got `{value}`. Proceed without serialization.")
            # Если возникает AttributeEroor - пишем в лог, отправляем сообщение без сериализации
            except AttributeError as e:
                self.logger.error(
                    f"{e}. Unkowns dataclass `{dataclass}`. Proceed without serialization."
                )
            # При возникновении неизвестной ошибки - пишем в лог, отправляем сообщение без сериализации
            except Exception as e:
                self.logger.error(f"Unexpected serialization error: {e}")

        return bytes_key, bytes_value

    def produce(
        self, topic: str, key: str | int | None, value: str, headers: dict[str, str]
    ) -> None:
        """Отправка сообщения в Kafka.

        Parameters
        ----------
        topic : str
            Топик в который отправляется сообщение
        key : str | int | None
            Ключ сообщения в виде строки, числа или пустое значение
        value : bytes
            Значение сообщения в виде байтов
        headers : dict[str, str]
            Заголовки сообщения в виде dict'а
        """

        # Задаем переменные и сохраняем в них сериализованные данные
        bytes_key: bytes | None
        bytes_value: bytes
        bytes_key, bytes_value = self.serialize_message(key, value, headers.get("dataclass", None))

        self.producer.produce(
            topic=topic,
            key=bytes_key,
            value=bytes_value,
            headers=headers,
            on_delivery=self.delivery_report,
        )

    def flush(self) -> None:
        """Проверяет что все сообщеня доставлены.
        Опрашивает очередь доставки сообщений пока ее длинна не станет 0.
        """
        self.producer.flush()
