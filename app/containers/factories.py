from dishka import (
	AsyncContainer,
	make_async_container,
)

from containers.providers import DefaultProvider


_container: AsyncContainer | None = None


def get_container() -> AsyncContainer:
	"""App-scoped container singleton for the bot process."""
	global _container
	if _container is None:
		_container = make_async_container(DefaultProvider())
	return _container
