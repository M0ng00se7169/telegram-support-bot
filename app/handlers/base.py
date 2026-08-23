from settings import get_settings
from telegram import Update
from telegram.ext import ContextTypes

from services.notifier import BaseNotifier


def _request_scope(context: ContextTypes.DEFAULT_TYPE):
	container = context.application.bot_data['container']
	return container()


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
	settings = get_settings()
	async with _request_scope(context) as request_container:
		notifier = await request_container.get(BaseNotifier)
		await notifier.acknowledge(
			tg_chat_id=update.effective_chat.id,
			text=settings.GREETINGS_TEXT,
		)
