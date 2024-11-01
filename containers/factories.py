from dishka import AsyncContainer, make_async_container

from containers.providers import DefaultProvider


def get_container() -> AsyncContainer:
    return make_async_container(DefaultProvider())
