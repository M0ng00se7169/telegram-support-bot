from telegram import Update
from telegram.ext import ContextTypes

from handlers.base import _request_scope
from handlers.constants import SEND_MESSAGE_STATE
from services.notifier import BaseNotifier
from services.replies import BaseReplyRegistry
from services.web import BaseChatDirectory


async def get_all_chats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
	async with _request_scope(context) as request_container:
		directory = await request_container.get(BaseChatDirectory)
		chats = await directory.get_all_chats()
		notifier = await request_container.get(BaseNotifier)
		await notifier.notify_chat_list(
			tg_chat_id=update.effective_chat.id,
			chats=chats,
		)


async def set_chat_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
	async with _request_scope(context) as request_container:
		directory = await request_container.get(BaseChatDirectory)
		await directory.add_listener(
			telegram_chat_id=update.effective_chat.id,
			chat_oid=context.args[0],
		)
		notifier = await request_container.get(BaseNotifier)
		await notifier.acknowledge(
			tg_chat_id=update.effective_chat.id,
			text='You connected to the chat.',
		)


async def start_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
	async with _request_scope(context) as request_container:
		notifier = await request_container.get(BaseNotifier)
		await notifier.acknowledge(
			tg_chat_id=update.effective_chat.id,
			text=(
				'Now you are answering questions. Please choose the message and reply on it.'
				'User will see your answer on the web site.'
			),
		)

	return SEND_MESSAGE_STATE


async def quit_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
	async with _request_scope(context) as request_container:
		notifier = await request_container.get(BaseNotifier)
		await notifier.acknowledge(
			tg_chat_id=update.effective_chat.id,
			text='You quited from the dialogue with client',
		)


async def send_message_to_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
	async with _request_scope(context) as request_container:
		notifier = await request_container.get(BaseNotifier)
		if update.message.reply_to_message is None:
			await notifier.acknowledge(
				tg_chat_id=update.effective_chat.id,
				text='Error! Please choose the message you want to reply.',
			)
			return

		reply_registry = await request_container.get(BaseReplyRegistry)
		chat_oid = await reply_registry.resolve(
			update.message.reply_to_message.message_id
		)
		if chat_oid is None:
			await notifier.acknowledge(
				tg_chat_id=update.effective_chat.id,
				text=(
					'Please reply to a chat notification (or the record has expired).'
					'Reply to the bot message about a new chat message.'
				),
			)
			return

		# Доставка reply в чат — следующая карточка; chat_oid подтверждён реестром.
		await notifier.acknowledge(
			tg_chat_id=update.effective_chat.id,
			text=f'Reply accepted for chat {chat_oid}.',
		)
