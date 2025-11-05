"""Сериализация / Десериализация с помощью dataclasses_avroschema."""

from abc import abstractmethod
from dataclasses import dataclass

from dataclasses_avroschema import AvroModel


class BaseMessage(AvroModel):
    @property
    @abstractmethod
    def partition_key(self) -> str: ...


@dataclass
class SKU(BaseMessage):
    """Dataclass реализующий единицу складского учета

    Parameters
    ----------
    sku_id: int
        Идентификатор единицы складского учета
    unit: str
        Тип единичы хранения (ящик, бутылка и т.п.)
    store: list[int]
        Идентификаторы плащадок хранения
    is_limited: bool
        Признак ограниченного наличия
    metadata: dict[str, str]
        Метаданные записи
    tag: list[str] | None
        Опциональные теги
    """

    sku_id: int
    unit: str
    store: list[int]
    is_limited: bool
    metadata: dict[str, str]
    tags: list[str] | None = None

    @property
    def partition_key(self) -> str:
        return "sku_id"
