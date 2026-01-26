from dishka import (
	AsyncContainer,
	make_async_container,
)
from dishka.integrations.faststream import setup_dishka
from faststream import FastStream
from faststream.kafka.broker import KafkaBroker

from containers.providers import DefaultProvider
from consumers.handlers import router
from settings import get_settings


_container: AsyncContainer | None = None


def get_container() -> AsyncContainer:
	"""App-scoped container singleton for the consumer process."""
	global _container
	if _container is None:
		_container = make_async_container(DefaultProvider())
	return _container


def get_app() -> FastStream:
	settings = get_settings()
	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	broker.include_router(router=router)
	app = FastStream(broker=broker)

	# Request scope per incoming message; container closed on after_shutdown.
	setup_dishka(container=get_container(), app=app)

	return app
