from dishka.integrations.faststream import (
	FromDishka,
	inject,
)
from faststream.kafka import KafkaRouter
from settings import get_settings

from consumers.schemas import NewChatMessageSchema
from services.notifier import BaseNotifier
from services.web import BaseChatDirectory


settings = get_settings()
router = KafkaRouter()


@router.subscriber(settings.NEW_MESSAGE_TOPIC, group_id=settings.KAFKA_GROUP_ID)
@inject
async def new_message_subscription_handler(
	message: NewChatMessageSchema,
	directory: FromDishka[BaseChatDirectory],
	notifier: FromDishka[BaseNotifier],
):
	# Реальный продюсер кладёт в key event_id, а не chat_oid:
	# единственный надёжный источник chat_oid — payload события.
	chat_oid = message.chat_oid

	listeners = await directory.get_chat_listeners(chat_oid=chat_oid)
	chat_info = await directory.get_chat_info(chat_oid=chat_oid)

	for listener in listeners:
		# Формат уведомления, parse_mode и запись в Reply Registry — забота
		# Notifier'а; здесь остаётся только бизнес-факт: кому и о чём сообщать.
		await notifier.notify_new_message(
			tg_chat_id=listener.oid,
			chat_oid=chat_oid,
			title=chat_info.title,
			text=message.message_text,
		)
