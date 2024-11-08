from faststream import Context
from faststream.kafka import KafkaRouter
from settings import get_settings
from telegram import Bot

from consumers.schemas import NewChatMessageSchema
from containers.factories import get_container
from services.web import BaseChatWebService


settings = get_settings()
router = KafkaRouter()


@router.subscriber(settings.NEW_MESSAGE_TOPIC, group_id=settings.KAFKA_GROUP_ID)
async def new_message_subscription_handler(
    message: NewChatMessageSchema,
    key: bytes = Context("message.raw_message.key"),
):
    container = get_container()

    async with container() as request_container:
        service = await request_container.get(BaseChatWebService)   # type: ignore
        listeners = await service.get_chat_listeners(chat_oid=key.decode())
        chat_info = await service.get_chat_info(chat_oid=key.decode())

        bot = await request_container.get(Bot)

        for listener in listeners:
            await bot.send_message(
                chat_id=listener.oid,
                text=(
                    f'Message from chat <b>{chat_info.oid}</b> (<code>{chat_info.title}</code>)'
                    f'<blockquote>{message.message_text}</blockquote>'
                ),
                parse_mode='HTML',
            )
