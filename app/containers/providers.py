from collections.abc import AsyncIterator

from dishka import (
	provide,
	Provider,
	Scope,
)
from httpx import AsyncClient
from telegram import Bot

from settings import ProjectSettings
from services.web import (
	BaseChatWebService,
	ChatWebService,
)


class DefaultProvider(Provider):
	@provide(scope=Scope.APP)
	def get_settings(self) -> ProjectSettings:
		return ProjectSettings()

	@provide(scope=Scope.REQUEST)
	async def get_http_client(self) -> AsyncIterator[AsyncClient]:
		async with AsyncClient() as http_client:
			yield http_client

	@provide(scope=Scope.REQUEST)
	def get_chat_web_service(
		self,
		http_client: AsyncClient,
		settings: ProjectSettings,
	) -> BaseChatWebService:
		return ChatWebService(
			http_client=http_client,
			base_url=settings.WEB_API_BASE_URL,
		)

	@provide(scope=Scope.APP)
	async def get_telegram_bot(self, settings: ProjectSettings) -> AsyncIterator[Bot]:
		bot = Bot(token=settings.TG_BOT_TOKEN)
		await bot.initialize()
		try:
			yield bot
		finally:
			await bot.shutdown()
