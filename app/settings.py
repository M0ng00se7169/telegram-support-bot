from functools import lru_cache

import environ
from pydantic_settings import BaseSettings


env = environ.Env()
environ.Env.read_env('.env')


class ProjectSettings(BaseSettings):
	TG_BOT_TOKEN: str = env('TG_BOT_TOKEN')
	GREETINGS_TEXT: str = env(
		'GREETINGS_TEXT',
		default=(
			'Welcome to the support bot.\n'
			'Please, choose chat to work with client.\nRetrieve list of all chats: '
			'/chats, Choose chat /set_chats <oid chat>.'
		),
	)
	WEB_API_BASE_URL: str = env('WEB_API_BASE_URL', default='http://main-app:8000')
	KAFKA_BROKER_URL: str = env('KAFKA_BROKER_URL', default='kafka:29092')
	NEW_MESSAGE_TOPIC: str = env('NEW_MESSAGE_TOPIC', default='new-messages')
	KAFKA_GROUP_ID: str = env('KAFKA_GROUP_ID', default='tg-bot')
	# Топики событий жизненного цикла чата и отдельная, третья consumer group
	# для подписки на них (спека): сервер использует свою группу, new-message —
	# свою; переиспользовать чужие группы нельзя — подписки бы конфликтовали.
	NEW_CHATS_TOPIC: str = env('NEW_CHATS_TOPIC', default='new-chats-topic')
	CHAT_DELETED_TOPIC: str = env('CHAT_DELETED_TOPIC', default='chat-deleted-topic')
	CHAT_EVENTS_GROUP_ID: str = env('CHAT_EVENTS_GROUP_ID', default='tg-bot-chat-events')
	VALKEY_URL: str = env('VALKEY_URL', default='valkey://valkey:6379/0')
	EVENT_DEDUP_TTL: int = env('EVENT_DEDUP_TTL', default=24 * 60 * 60)


@lru_cache(1)
def get_settings() -> ProjectSettings:
	return ProjectSettings()
