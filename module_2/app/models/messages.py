"Классы реализующие объекты сообщений в системе"

import faust

class Message(faust.Record, serializer="json"):
    """Простое сообщение

    Fields
    ------
    sender_id : str
        Идентификатор отправителя сообщения
    reciver_id: str
        Идентификатор получателя сообщения
    body : str
        Тело сообщения
    """
    sender_id: str
    reciver_id: str
    body: str