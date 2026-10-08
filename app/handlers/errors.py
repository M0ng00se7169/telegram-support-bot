from telegram import Update
from telegram.ext import CallbackContext

from exceptions.base import ApplicationException
from exceptions.web import WebApiError
from handlers.base import _request_scope
from services.notifier import BaseNotifier


async def error_handler(update: Update, context: CallbackContext) -> None:
	try:
		raise context.error  # type: ignore
	except WebApiError as error:
		text = '\n'.join(part for part in (error.message, error.detail) if part)
	except ApplicationException as error:
		print(error.meta)
		text = error.message
	else:
		return

	message = update.effective_message
	if message is None:
		chat = update.effective_chat
		if chat is None:
			return
		tg_chat_id = chat.id
		reply_to_message_id = None
	else:
		tg_chat_id = message.chat.id
		reply_to_message_id = message.message_id

	async with _request_scope(context) as request_container:
		notifier = await request_container.get(BaseNotifier)
		await notifier.acknowledge(
			tg_chat_id=tg_chat_id,
			text=text,
			reply_to_message_id=reply_to_message_id,
		)
