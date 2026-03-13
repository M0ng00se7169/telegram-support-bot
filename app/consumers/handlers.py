from dishka.integrations.faststream import (
	FromDishka,
	inject,
)
from faststream.kafka import KafkaRouter
from settings import get_settings
from telegram import Bot

from consumers.schemas import NewChatMessageSchema
from services.web import BaseChatDirectory


settings = get_settings()
router = KafkaRouter()


@router.subscriber(settings.NEW_MESSAGE_TOPIC, group_id=settings.KAFKA_GROUP_ID)
@inject
async def new_message_subscription_handler(
	message: NewChatMessageSchema,
	directory: FromDishka[BaseChatDirectory],
	bot: FromDishka[Bot],
):
	# Kafka key продюсера содержит event_id, а не chat_oid, поэтому запрос по
	# key.decode() уходил в 404 и ронял всю обработку события.
	chat_oid = message.chat_oid

	listeners = await directory.get_chat_listeners(chat_oid=chat_oid)
	chat_info = await directory.get_chat_info(chat_oid=chat_oid)

	for listener in listeners:
		await bot.send_message(
			chat_id=listener.oid,
			text=(
				f'New message in chat <b>{chat_info.title}</b>'
				f'<blockquote>{message.message_text}</blockquote>'
			),
			parse_mode='HTML',
		)
