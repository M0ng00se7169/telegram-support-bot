"""Chat Title Cache: заголовки чата без N+1 в горячем пути (карточка №5).

Deep Module: скрывает три источника заполнения и read-through за четырьмя операциями —
`warm_up` (прогрев при старте), `remember` (событие создания), `forget`
(событие удаления) и `get_title` (чтение с точечным промахом).

Кэш **локален для процесса consumer'а** и намеренно без отдельной абстракции
и без Valkey: заголовки нужны только consumer'у, единственный потребитель не
требует общего бэкенда — вводить его ради одного процесса значит создать seam
без второго пользователя (решение спеки). Проверяемость даёт существующий
Chat Directory: кэш читает через него, а directory передаётся аргументом —
поэтому APP-скоупный кэш не зависит от REQUEST-скоупного directory (правило
скоупов dishka).

Источники заполнения:
  1. прогрев при старте — одним запросом за список чатов (`warm_up`);
  2. событие создания чата — `remember` (единственный серверный источник
     заголовка; держит кэш тёплым без перезапуска процесса);
  3. событие удаления чата — `forget`: явный tombstone, `get_title` вернёт
     `None`, и уведомление по несуществующему чату будет пропущено ДО
     любого HTTP-запроса.

Промах — точечный read-through: один `GET /chat/{oid}/`, результат кладётся
в кэш, второе обращение к тому же чату запросов не делает. Если Web API чат
не отдаёт (жёсткое удаление без tombstone, транзиентный сбой) — возвращается
сам `chat_oid` запасным заголовком: отсутствующий или устаревший заголовок
никогда не является причиной не отправлять уведомление («молчание хуже
неточности»). Новых типов исключений модуль не вводит (ADR-0004): сбой
чтения поглощается и логируется предупреждением.

Переименование чата не отслеживается: сервер не публикует такие события,
заголовок в уведомлении может устареть — это принятый компромисс.
"""

import logging
from dataclasses import (
	dataclass,
	field,
)

from services.web import BaseChatDirectory


logger = logging.getLogger(__name__)


@dataclass
class ChatTitleCache:
	"""In-process кэш `chat_oid → title` c tombstone-состоянием удаления."""

	_titles: dict[str, str] = field(default_factory=dict)
	_deleted: set[str] = field(default_factory=set)

	async def warm_up(self, directory: BaseChatDirectory) -> None:
		"""Прогрев одним запросом за список чатов. Никогда не бросает.

		Сбой прогрева не должен блокировать подписку: consumer продолжает
		принимать события, кэш доберёт значения read-through'ом по мере
		надобности, поэтому исключение любого типа логируется и глотается.
		"""
		try:
			chats = await directory.get_all_chats()
		except Exception as error:
			logger.warning('Прогрев Chat Title Cache не удался: %s', error)
			return
		for chat in chats:
			self.remember(chat.oid, chat.title)

	def remember(self, chat_oid: str, title: str) -> None:
		"""Кладёт заголовок и снимает tombstone (событие создания чата)."""
		self._titles[chat_oid] = title
		self._deleted.discard(chat_oid)

	def forget(self, chat_oid: str) -> None:
		"""Убирает заголовок и помечает чат удалённым (событие удаления).

		Tombstone — единственная законная причина пропустить уведомление:
		чата больше нет, отправлять некому и не о чем.
		"""
		self._titles.pop(chat_oid, None)
		self._deleted.add(chat_oid)

	async def get_title(
		self,
		chat_oid: str,
		directory: BaseChatDirectory,
	) -> str | None:
		"""Заголовок чата: из кэша, на промахе — точечным read-through'ом.

		`None` — только для удалённого чата (tombstone): вызывающий пропускает
		уведомление целиком, до HTTP. Любой сбой чтения (Web API не знает чат
		или недоступен) возвращает `chat_oid` запасным заголовком и
		предупреждением в лог: метод не бросает, уведомление не теряется.
		"""
		if chat_oid in self._deleted:
			return None
		cached = self._titles.get(chat_oid)
		if cached is not None:
			return cached
		try:
			chat_info = await directory.get_chat_info(chat_oid=chat_oid)
		except Exception as error:
			logger.warning(
				'Заголовок чата недоступен, беру chat_oid запасным: %s',
				error,
			)
			return chat_oid
		self.remember(chat_oid, chat_info.title)
		return chat_info.title
