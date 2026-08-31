"""Notifier — единственная точка отправки в Telegram (карточка №3).

Deep Module: наружу — бизнес-операции («уведомить о новом сообщении», «показать
список чатов», «подтвердить действие»), внутри — `Bot`, формат сообщения,
разметка и экранирование. Ни один потребитель больше не задаёт `parse_mode` и
не собирает текст уведомления сам.

Пара «TG-уведомление → chat_oid» для Reply Registry рождается здесь же: только
после успешной отправки, потому что `message_id` доставленного сообщения
известен лишь по ответу Bot API. TTL записи задаёт `services.replies.py`.
"""

from abc import (
	ABC,
	abstractmethod,
)
from collections.abc import Sequence
from dataclasses import dataclass
from html import escape

from telegram import Bot
from telegram.helpers import escape_markdown

from dtos.messages import ChatListItemDTO
from services.replies import BaseReplyRegistry

# Bot API принимает адресата и как `int`, и как `str`: у групп и каналов id
# отрицательный, а каналы адресуются `@username`. Источники в проекте тоже
# разные: Web API отдаёт Chat Listener как строку, PTB отдаёт id апдейта как int.
TelegramAddress = int | str


def _escape_markdown_code(value: str) -> str:
	"""Экранирует только зарезервированные символы MarkdownV2 внутри code."""
	return value.replace('\\', '\\\\').replace('`', '\\`')


class BaseNotifier(ABC):
	@abstractmethod
	async def notify_new_message(
		self,
		tg_chat_id: TelegramAddress,
		chat_oid: str,
		title: str,
		text: str,
	) -> None: ...

	@abstractmethod
	async def notify_chat_list(
		self,
		tg_chat_id: TelegramAddress,
		chats: Sequence[ChatListItemDTO],
	) -> None: ...

	@abstractmethod
	async def acknowledge(
		self,
		tg_chat_id: TelegramAddress,
		text: str,
		reply_to_message_id: int | None = None,
	) -> None: ...


@dataclass
class TelegramNotifier(BaseNotifier):
	"""Единственный адаптер Telegram; `Bot` приходит снаружи, формат — отсюда."""

	bot: Bot
	reply_registry: BaseReplyRegistry

	async def notify_new_message(
		self,
		tg_chat_id: TelegramAddress,
		chat_oid: str,
		title: str,
		text: str,
	) -> None:
		# HTML выбран для уведомления о новом сообщении: blockquote, который
		# цитирует реплику клиента, в MarkdownV2 пришлось бы городить вручную.
		# В PTB 21.6 нет хелпера экранирования HTML, поэтому берём stdlib
		# `html.escape`: quote=False экранирует ровно те три символа, которые
		# Telegram считает зарезервированными в HTML (&, <, >), и не портит
		# читаемость кавычками.
		sent_message = await self.bot.send_message(
			chat_id=tg_chat_id,
			text=(
				f'New message in chat <b>{escape(title, quote=False)}</b>'
				f'<blockquote>{escape(text, quote=False)}</blockquote>'
			),
			parse_mode='HTML',
		)
		await self.reply_registry.remember(
			message_id=sent_message.message_id,
			chat_oid=chat_oid,
		)

	async def notify_chat_list(
		self,
		tg_chat_id: TelegramAddress,
		chats: Sequence[ChatListItemDTO],
	) -> None:
		header = '*List of all available chats:*'
		if not chats:
			text = f'{header}\n_{escape_markdown("No chats yet.", version=2)}_'
		else:
			blocks = [
				'\n'.join(
					(
						f'ChatOID: `{_escape_markdown_code(chat.oid)}`',
						f'Problem: *{escape_markdown(chat.title, version=2)}*',
					),
				)
				for chat in chats
			]
			text = '\n'.join([header, *blocks])

		await self.bot.send_message(
			chat_id=tg_chat_id,
			text=text,
			parse_mode='MarkdownV2',
		)

	async def acknowledge(
		self,
		tg_chat_id: TelegramAddress,
		text: str,
		reply_to_message_id: int | None = None,
	) -> None:
		if reply_to_message_id is None:
			await self.bot.send_message(chat_id=tg_chat_id, text=text)
		else:
			await self.bot.send_message(
				chat_id=tg_chat_id,
				text=text,
				reply_to_message_id=reply_to_message_id,
			)
