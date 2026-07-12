from telegram.helpers import escape_markdown

from dtos.messages import ChatListItemDTO


def _escape_code(value: str) -> str:
	"""Экранирование внутри code-сущности MarkdownV2.

	Внутри `code` зарезервированы только обратный слэш и сама кавычка,
	поэтому `-` и `.` в UUID экранировать не нужно — иначе они попадут
	в сообщение вместе со слэшами.
	"""
	return value.replace('\\', '\\\\').replace('`', '\\`')


def convert_chats_dtos_to_message(chats: list[ChatListItemDTO]) -> str:
	"""MarkdownV2-разметка списка чатов.

	Статическая разметка пишется руками, динамические значения (oid, title)
	приходят из Web API и содержат любые символы, поэтому экранируются:
	внутри `code` — только слэш и кавычка, в обычном тексте — полный набор
	зарезервированных символов MarkdownV2.
	"""
	header = '*List of all available chats:*'
	if not chats:
		return f'{header}\n_{escape_markdown("No chats yet.", version=2)}_'

	blocks = [
		'\n'.join(
			(
				f'ChatOID: `{_escape_code(chat.oid)}`',
				f'Problem: *{escape_markdown(chat.title, version=2)}*',
			),
		)
		for chat in chats
	]
	return '\n'.join([header, *blocks])
