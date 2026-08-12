from settings import get_settings
from telegram.ext import (
	ApplicationBuilder,
	CommandHandler,
	ConversationHandler,
	filters,
	MessageHandler,
)
from telegram.request import BaseRequest

from containers.factories import get_container
from handlers.base import start_handler
from handlers.chats import (
	get_all_chats_handler,
	quit_chat,
	send_message_to_chat,
	set_chat_listener,
	start_dialog,
)
from handlers.constants import SEND_MESSAGE_STATE
from handlers.errors import error_handler


def get_app(request: BaseRequest | None = None):
	settings = get_settings()
	container = get_container()

	async def close_container(_application):
		await container.close()

	builder = ApplicationBuilder().token(token=settings.TG_BOT_TOKEN)
	# Необязательный крючок: офлайн-смоуки подставляют заглушку транспорта Telegram,
	# чтобы настоящий Bot работал без сети. Без аргумента поведение прежнее.
	if request is not None:
		# Оба канала Bot получают одну заглушку: канал getUpdates иначе остался бы
		# реальным HTTPXRequest и увёл бы смоук в сеть.
		builder = builder.request(request).get_updates_request(request)

	application = builder.post_shutdown(close_container).build()
	application.bot_data['container'] = container

	start_command_handler = CommandHandler('start', start_handler)
	get_all_chats_command_handler = CommandHandler('chats', get_all_chats_handler)
	set_chat_listener_handler = CommandHandler('listen_chat', set_chat_listener)
	start_dialog_handler = CommandHandler('start_dialog', start_dialog)
	quit_chat_handler = CommandHandler('quit', quit_chat)
	send_chat_messages_handler = ConversationHandler(
		entry_points=[start_dialog_handler],
		states={
			SEND_MESSAGE_STATE: [
				MessageHandler(
					filters=filters.TEXT & ~filters.COMMAND,
					callback=send_message_to_chat,
				),
			],
		},
		fallbacks=(quit_chat_handler,),
	)
	application.add_handler(start_command_handler)
	application.add_handler(get_all_chats_command_handler)
	application.add_handler(set_chat_listener_handler)
	application.add_handler(send_chat_messages_handler)
	application.add_error_handler(error_handler, block=True)

	print('bot started')

	return application


if __name__ == '__main__':
	get_app().run_polling()
