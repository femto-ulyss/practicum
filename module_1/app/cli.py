"""Интерфес командной строки."""

import argparse
from typing import Any

from main import consumer, producer  # type: ignore

# Справочник возможных процессов
processes: dict[str, Any] = {"consumer": consumer, "producer": producer}

if __name__ == "__main__":
    # Базовый парсер агрументов
    parser: argparse.ArgumentParser = argparse.ArgumentParser("Produce or consume from Kafka")

    # Сабпарсеры зависимые от первого позиционног аргумента
    subparsers = parser.add_subparsers(dest="process", help="consumer | producer")

    # Сабпарсер для запуска consumer'а
    consumer_parse = subparsers.add_parser("consumer", help="Consumer from Kafka")
    consumer_parse.add_argument("--group_id", "-g", help="Consumer group_id")
    consumer_parse.add_argument(
        "--subscribe", "-s", help="Comma separeted list of topics to subscribe"
    )
    consumer_parse.add_argument(
        "--batch", "-b", help="Switch to batch consumer", action="store_true"
    )

    # Сабпарсер для запуска producer'а
    producer_parse = subparsers.add_parser("producer", help="Produce to Kafka")
    producer_parse.add_argument("--topic", "-t", help="To which topic message should be written")
    producer_parse.add_argument("--key", "-k", help="Optional message key", default=None)
    producer_parse.add_argument("--value", "-v", help="Value to produce")
    producer_parse.add_argument(
        "--dataclass", "-d", help="Dataclass from app.messages custom serialization", default=None
    )

    # Парсинг аргументов
    args: argparse.Namespace = parser.parse_args()

    # Вызов небходимого процесс с аргументами
    processes.get(args.process, "consumer")(**{k: v for k, v in args._get_kwargs()[1:]})
