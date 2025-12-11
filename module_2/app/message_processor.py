"Основной модуль"

import faust
import logging
import sys
from pathlib import Path

project_dir: Path = Path(__file__).parent
if project_dir not in sys.path:
    sys.path.insert(0, str(project_dir))

from models.messages import Message  # type: ignore  # noqa: E402
from models.utils import SenderBlock, WordBlock  # type: ignore # noqa: E402
from utils.censor import mask_word  # type: ignore # noqa: E402

log: logging.Logger = logging.getLogger(__name__)

# Создаем приложение faust
app: faust.App = faust.App("message-stream-processor", broker="localhost:9092,localhost:9093,localhost:9094")

# Создаем основной входящий (`messages`) и исходящий (`filtered_messages`) топики
input_message_topic: faust.TopicT = app.topic("messages", key_type=str, value_serializer=Message)
filtered_message_topic: faust.TopicT = app.topic("filtered_messages", key_type=str, value_serializer=Message)

# Создаем сервисные топики (`blocked_users`, `blocked_words`, `blocked_messages`)
input_user_block_topic: faust.TopicT = app.topic("blocked_users", key_type=str, value_serializer=SenderBlock)
input_word_block_topic: faust.TopicT = app.topic("blocked_words", key_type=str, value_serializer=WordBlock)
blocked_messages: faust.TopicT = app.topic("blocked_messages", key_type=str, value_serializer=Message)

# Создаем таблицы для хранения состояний заблокированных пользователей (`blocked_users`) 
# и цензурируемых слова (`blocked_words`)
blocked_users_table: faust.types.tables.TableT = app.Table("blocked_users", default=set, partitions=3)
blocked_words_table: faust.types.tables.TableT = app.Table("blocked_words", default=bool, partitions=3)

# Создаем агенты

@app.agent(input_message_topic)
async def process_message(messages: faust.StreamT) -> None:
    """Агент обрабатывающий поток из основного входящего топика: `messages`. Проверяет блокировку
    со стороны получателя и маскирует цензурируемые слова в теле сообщения.

    Если отправка заблокирована записывает сообщение в сервисный топик `blocked_messages`, иначе
    записывает сообщения в `filtered_messages`.

    Если в теле сообщения присутсвуют цензурируемые слова маскирует их `*` от второго до предпоследнего
    символа.

    Parameters
    ----------
    messages : faust.StreamT
        Поток сообщений в топике `messages`
    """
    # Итерируемы по событиям в потоке `messages`
    async for message in messages:
        log.info("Recive new message")

        # Проверяем тело сообщения на наличие цензурируемых слов
        for k, v in blocked_words_table.items():
            # Если слово есть в таблице `blocked_words` со статусом `True` и в теле сообщения
            if v and k.lower() in message.body.lower():
                # Пищем в лог
                log.info("Word %s is blocked.", k)
                
                # Маскируем слово в теле сообшения
                message.body = mask_word(message.body, k)

        # Если отправка заблокирована получателем
        if message.sender_id in blocked_users_table[message.reciver_id]:
            # Пищем в лог
            log.info("Message from %s blocked by %s", message.sender_id, message.reciver_id)
            
            # Пишем в топик заблокированных сообщений
            await blocked_messages.send(value=message)
            # Переходим к следующему событию из потока `messages`
            continue

        # Пишем сообщение в топик `filtered_messages`
        await filtered_message_topic.send(value=message)

@app.agent(input_user_block_topic)
async def block_user(blocked_users: faust.StreamT) -> None:
    """Агент обрабатывает сообщения в топик `blocked_users`. При получени
    сообщения о блокировке собщение записывает в таблицу `blocked_users`.

    Parameters
    ----------
    user_blocks : faust.StreamT
        Поток сообщений в топике `blocked_users`
    """
    # Итерируемы по событиям в потоке `blocked_users`
    async for block in blocked_users:
        log.info("Blocking user: %s for user: %s", block.block, block.user)
        
        # Получаем из таблицы текущий набор блокировок
        block_set: set = blocked_users_table[block.user]
        # Добавляем нового заблокированного пользователя 
        block_set.add(block.block)
        # Записываем обновленный набор блокировок в таблицы `blocked_users`
        blocked_users_table[block.user] = block_set

@app.agent(input_word_block_topic)
async def block_word(blocked_words: faust.StreamT) -> None:
    """Агент обрабатывает сообщения в топик `blocked_words`. Получении сообщения
    о цензурировании слова оно записывается в таблицу `blocked_words`

    Parameters
    ----------
    word_blocks : faust.StreamT
        Поток сообщений в топике `blocked_words`
    """
    # Итерируемы по событиям в потоке `blocked_users`
    async for block in blocked_words:
        log.info("Word `%s` add to block list", block.word)
        # Пищем слово в таблицу `blocked_words`
        blocked_words_table[block.word] = True
        