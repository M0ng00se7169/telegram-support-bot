"""Точка входа consumer'а: FastStream-приложение и прогрев Chat Title Cache."""

import logging

from dishka import (
	AsyncContainer,
	make_async_container,
)
from dishka.integrations.faststream import setup_dishka
from faststream import FastStream
from faststream.kafka.broker import KafkaBroker

from containers.providers import DefaultProvider
from consumers.handlers import router
from services.title_cache import ChatTitleCache
from services.web import BaseChatDirectory
from settings import get_settings


logger = logging.getLogger(__name__)

_container: AsyncContainer | None = None


def get_container() -> AsyncContainer:
	"""App-scoped container singleton for the consumer process."""
	global _container
	if _container is None:
		_container = make_async_container(DefaultProvider())
	return _container


async def warm_title_cache(container: AsyncContainer) -> None:
	"""Прогревает Chat Title Cache одним запросом за список чатов.

	Входит в request scope вручную — тот же приём, что `_request_scope` у
	бота: Chat Directory — REQUEST-скоуп, кэш — APP-скоуп, поэтому directory
	резолвится в scope'е и передаётся в кэш аргументом.

	Слоистая защита прогрева: внутренний `warm_up` ловит только граничные
	сбои Web API/парсинга и даёт багам всплыть (ADR-0004), а этот хук —
	широкая страховка на старте: ловит ВСЁ, включая всплывшие оттуда баги
	и отказы DI, потому что сбой прогрева при старте не должен блокировать
	подписку (билет 03, п. 11). Сообщение намеренно отличается от
	внутреннего «…не удался» и несёт трейсбек — видно, какой слой сработал
	и что именно помешало прогреву.
	"""
	try:
		async with container() as scope:
			directory = await scope.get(BaseChatDirectory)
			cache = await scope.get(ChatTitleCache)
			await cache.warm_up(directory)
	except Exception as error:
		logger.warning(
			'Прогрев Chat Title Cache прерван непредвиденной ошибкой — '
			'подписка продолжается без прогрева: %r',
			error,
			exc_info=True,
		)


def get_app() -> FastStream:
	settings = get_settings()
	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	broker.include_router(router=router)
	app = FastStream(broker=broker)

	# Request scope per incoming message; container closed on after_shutdown.
	container = get_container()
	setup_dishka(container=container, app=app)

	# Прогрев кэша — хук on_startup: он выполняется ДО подключения к брокеру,
	# поэтому кэш прогревается раньше, чем пойдут события, и гонки между
	# прогревом и подписками жизненного цикла чатов не бывает. Сбой прогрева
	# не блокирует запуск — warm_title_cache не бросает.
	async def _warm_title_cache_on_startup() -> None:
		await warm_title_cache(container)

	app.on_startup(_warm_title_cache_on_startup)

	return app
