from dataclasses import (
    asdict,
    dataclass,
)


# Не frozen: contextlib при пробросе исключения через менеджер контекста
# делает `exc.__traceback__ = tb`, а frozen-dataclass __setattr__ это запрещает
# и маскирует реальную ошибку под FrozenInstanceError.
@dataclass(eq=False)
class ApplicationException(Exception):
    @property
    def meta(self):
        return asdict(self)

    @property
    def message(self):
        return "An error occurred while application was running"
