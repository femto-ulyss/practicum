"Функции для цензурирования сообщения"

import re

def mask_word(text: str, pattern: str) -> str:
    """Функция маскирует заблокированне слова в теле сообщений.

    Parameters
    ----------
    text : str
        Тело сообщеиния
    pattern : str
        Паттерн для маскирвания внутри тела сообщения

    Returns
    -------
    str
        Тело сообщения с маскированными заблокированные словами
    """

    escaped_pattern: str = re.escape(pattern)

    return re.sub(
        escaped_pattern, lambda m: m.group()[0] + "*" * (len(m.group()) -2 ) + m.group()[-1],
        text,
        flags=re.IGNORECASE
    )