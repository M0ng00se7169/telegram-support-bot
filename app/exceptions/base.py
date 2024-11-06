from dataclasses import (
    asdict,
    dataclass,
)


@dataclass(frozen=True, eq=False)
class ApplicationException(Exception):
    @property
    def meta(self):
        return asdict(self)

    @property
    def message(self):
        return "An error occurred while application was running"
