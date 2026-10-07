from collections.abc import AsyncIterator

from dishka import (
	provide,
	Provider,
	Scope,
)
from httpx import AsyncClient
from telegram import Bot
from valkey.asyncio import ConnectionPool, Valkey

from settings import ProjectSettings
from services.dedup import (
	BaseEventDeduplicator,
	ValkeyEventDeduplicator,
)
from services.notifier import (
	BaseNotifier,
	TelegramNotifier,
)
from services.replies import BaseReplyRegistry, ValkeyReplyRegistry
from services.web import (
	BaseChatDirectory,
	ChatDirectory,
)


class DefaultProvider(Provider):
	@provide(scope=Scope.APP)
	def get_settings(self) -> ProjectSettings:
		return ProjectSettings()

	@provide(scope=Scope.REQUEST)
	async def get_http_client(self) -> AsyncIterator[AsyncClient]:
		async with AsyncClient() as http_client:
			yield http_client

	@provide(scope=Scope.APP)
	async def get_telegram_bot(self, settings: ProjectSettings) -> AsyncIterator[Bot]:
		bot = Bot(token=settings.TG_BOT_TOKEN)
		await bot.initialize()
		try:
			yield bot
		finally:
			await bot.shutdown()

	@provide(scope=Scope.APP)
	async def get_valkey_pool(
		self, settings: ProjectSettings
	) -> AsyncIterator[ConnectionPool]:
		pool = ConnectionPool.from_url(url=settings.VALKEY_URL)
		try:
			yield pool
		finally:
			await pool.disconnect()

	@provide(scope=Scope.APP)
	def get_reply_registry(self, pool: ConnectionPool) -> BaseReplyRegistry:
		return ValkeyReplyRegistry(client=Valkey(connection_pool=pool))

	@provide(scope=Scope.APP)
	def get_event_deduplicator(
		self,
		pool: ConnectionPool,
		settings: ProjectSettings,
	) -> BaseEventDeduplicator:
		return ValkeyEventDeduplicator(
			client=Valkey(connection_pool=pool),
			ttl=settings.EVENT_DEDUP_TTL,
		)

	# APP-скоуп: внутри Notifier'а живёт `Bot`, он тоже APP-скоуп.
	@provide(scope=Scope.APP)
	def get_notifier(
		self,
		bot: Bot,
		reply_registry: BaseReplyRegistry,
	) -> BaseNotifier:
		return TelegramNotifier(bot=bot, reply_registry=reply_registry)

	@provide(scope=Scope.REQUEST)
	def get_chat_directory(
		self,
		http_client: AsyncClient,
		settings: ProjectSettings,
	) -> BaseChatDirectory:
		return ChatDirectory(
			http_client=http_client,
			base_url=settings.WEB_API_BASE_URL,
		)
