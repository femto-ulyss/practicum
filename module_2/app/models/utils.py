"Утилитарные классы для записи сообещений в blocked_users и blocked_words"
import faust

class SenderBlock(faust.Record, serializer="json"):
    """Класс записи в топик blocked_users

    Fields
    ----------
    user : str
        Пользователь инициатор блокировки
    block : str
        Пользователь для которого будет заблокирована отправка сообщений
        к `user`
    """
    user: str
    block: str


class WordBlock(faust.Record, serializer="json"):
    """Класс записи в blocker_words

    Fields
    ------
    word : str
        Слово которо будет маскироваться в рамках обмена сообщениями
    """
    word: str