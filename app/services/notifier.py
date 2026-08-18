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

from dtos.messages import ChatListItemDTO
from services.replies import BaseReplyRegistry


class BaseNotifier(ABC):
	@abstractmethod
	async def notify_new_message(
		self,
		tg_chat_id: str,
		chat_oid: str,
		title: str,
		text: str,
	) -> None: ...

	@abstractmethod
	async def notify_chat_list(
		self,
		tg_chat_id: int,
		chats: Sequence[ChatListItemDTO],
	) -> None: ...

	@abstractmethod
	async def acknowledge(self, tg_chat_id: int, text: str) -> None: ...


@dataclass
class TelegramNotifier(BaseNotifier):
	"""Единственный адаптер Telegram; `Bot` приходит снаружи, формат — отсюда."""

	bot: Bot
	reply_registry: BaseReplyRegistry

	async def notify_new_message(
		self,
		tg_chat_id: str,
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
		tg_chat_id: int,
		chats: Sequence[ChatListItemDTO],
	) -> None:
		raise NotImplementedError

	async def acknowledge(self, tg_chat_id: int, text: str) -> None:
		raise NotImplementedError
