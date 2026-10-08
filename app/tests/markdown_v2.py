"""Инвариантная проверка корректности MarkdownV2 для офлайн-смоуков.

Notifier сам решает, что уходит в Telegram с `parse_mode=MarkdownV2`, поэтому
проверять корректность разметки нужно на выходе модуля, а не на его внутренностях.
PTB v21 не умеет парсить entities локально, поэтому проверка инвариантная:
Telegram отклоняет сообщение, в котором остался незаэкранированный
зарезервированный символ MarkdownV2.
"""

import re


# Символы, которые Telegram считает зарезервированными в MarkdownV2.
# Ищем незаэкранированные вхождения: перед символом не должно быть слэша.
_UNESCAPED_RESERVED = re.compile(r'(?<!\\)[_*\[\]()~`>#+\-=|{}.!]')
_CODE_SPAN = re.compile(r'`(?:\\.|[^`\\])*`')
# Наша собственная разметка: `*bold*`, `*italic*`, `_italic_`.
# Данные экранируются, поэтому неэкранированные `*` и `_` — только маркеры.
_OWN_MARKUP = re.compile(r'(?<!\\)[*_]')


def assert_valid_markdownv2(text: str) -> None:
	"""Telegram отклоняет сообщение с незаэкранированными зарезервированными
	символами MarkdownV2. PTB v21 не умеет парсить entities локально, поэтому
	проверяем инвариант вручную:

	- внутри `code` зарезервированы только слэш и кавычка — вырезаем такие
	  фрагменты целиком, они не должны валить сообщение;
	- неэкранированные `*` и `_` вне `code` — это наша разметка, убираем её;
	- всё остальное обязано быть экранировано.
	"""
	without_code = _CODE_SPAN.sub('', text)
	without_markup = _OWN_MARKUP.sub('', without_code)
	assert not _UNESCAPED_RESERVED.findall(
		without_markup,
	), f'unescaped MarkdownV2 reserved chars in {without_markup!r} (full: {text!r})'
