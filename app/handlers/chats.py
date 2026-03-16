from telegram import Update
from telegram.ext import ContextTypes

from handlers.constants import SEND_MESSAGE_STATE
from handlers.converters.chats import convert_chats_dtos_to_message
from services.replies import BaseReplyRegistry
from services.web import BaseChatDirectory


def _request_scope(context: ContextTypes.DEFAULT_TYPE):
	container = context.application.bot_data['container']
	return container()


async def get_all_chats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
	async with _request_scope(context) as request_container:
		directory = await request_container.get(BaseChatDirectory)
		chats = await directory.get_all_chats()

		await context.bot.send_message(
			chat_id=update.effective_chat.id,
			text=convert_chats_dtos_to_message(chats=chats),
			parse_mode='MarkdownV2',
		)


async def set_chat_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
	async with _request_scope(context) as request_container:
		directory = await request_container.get(BaseChatDirectory)
		await directory.add_listener(
			telegram_chat_id=update.effective_chat.id,
			chat_oid=context.args[0],
		)
		await context.bot.send_message(
			chat_id=update.effective_chat.id,  # type: ignore
			text='You connected to the chat.',
			parse_mode='MarkdownV2',
		)


async def start_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
	await context.bot.send_message(
		chat_id=update.effective_chat.id,
		text=(
			'Now you are answering questions. Please choose the message and reply on it.'
			'User will see your answer on the web site.'
		),
	)

	return SEND_MESSAGE_STATE


async def quit_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
	await context.bot.send_message(
		chat_id=update.effective_chat.id,
		text='You quited from the dialogue with client',
		parse_mode='MarkdownV2',
	)


async def send_message_to_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
	if update.message.reply_to_message is None:
		await context.bot.send_message(
			chat_id=update.effective_chat.id,  # type: ignore
			text='Error! Please choose the message you want to reply.',
		)
		return

	async with _request_scope(context) as request_container:
		reply_registry = await request_container.get(BaseReplyRegistry)
		chat_oid = await reply_registry.resolve(
			update.message.reply_to_message.message_id
		)

	if chat_oid is None:
		await context.bot.send_message(
			chat_id=update.effective_chat.id,  # type: ignore
			text=(
				'Please reply to a chat notification (or the record has expired).'
				'Reply to the bot message about a new chat message.'
			),
		)
		return

	# Доставка reply в чат — следующая карточка; chat_oid подтверждён реестром.
	await context.bot.send_message(
		chat_id=update.effective_chat.id,  # type: ignore
		text=f'Reply accepted for chat {chat_oid}.',
	)
