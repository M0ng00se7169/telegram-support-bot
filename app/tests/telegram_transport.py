"""Заглушка транспортного уровня Telegram для офлайн-смоуков.

PTB разделяет `Bot` и транспорт: единственное место, где появляется сеть —
`telegram.request.BaseRequest`. Заглушка реализует этот интерфейс, поэтому
настоящий `Bot` и настоящий код приложения работают без правок, а смоук
проверяет чёрный ящик «что ушло в Telegram», а не «какой метод `telegram.Bot`
подменён».
"""

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import (
    datetime,
    UTC,
)
from typing import Any

from dishka import (
    provide,
    Provider,
    Scope,
)
from settings import ProjectSettings
from telegram import Bot
from telegram.request import (
    BaseRequest,
    RequestData,
)


# Фиксированная дата ответа: смоукам не важно «сейчас», важна детерминированность.
_ANSWER_DATE = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp())

_BOT_USER: dict[str, Any] = {
	'id': 424_242,
	'is_bot': True,
	'first_name': 'Offline',
	'username': 'offline_bot',
}


def _chat_id(raw_chat_id: Any) -> int:
	"""`Chat.id` в ответе Telegram — всегда int."""
	try:
		return int(raw_chat_id)
	except (TypeError, ValueError):
		return 0


@dataclass(frozen=True)
class TelegramCall:
	"""Один исходящий вызов Bot API таким, каким его увидела заглушка."""

	method: str
	parameters: dict[str, Any]
	result: dict[str, Any]


class OfflineTelegramTransport(BaseRequest):
	"""Транспорт, который вместо сети отвечает синтетическими объектами Bot API.

	Записывает вызов целиком (метод, параметры, синтетический ответ), а `message_id`
	в ответе на `sendMessage` монотонно возрастает с 1 — как у настоящего API.
	"""

	def __init__(self) -> None:
		self.calls: list[TelegramCall] = []
		self._next_message_id = 1

	@property
	def sent_calls(self) -> list[TelegramCall]:
		"""Вызовы `sendMessage` в порядке отправки."""
		return [call for call in self.calls if call.method == 'sendMessage']

	@property
	def sent_messages(self) -> list[dict[str, Any]]:
		"""Параметры всех вызовов `sendMessage` в порядке отправки."""
		return [call.parameters for call in self.sent_calls]

	async def initialize(self) -> None:
		return None

	async def shutdown(self) -> None:
		return None

	async def do_request(
		self,
		url: str,
		method: str,
		request_data: RequestData | None = None,
		read_timeout: float | None = None,
		write_timeout: float | None = None,
		connect_timeout: float | None = None,
		pool_timeout: float | None = None,
	) -> tuple[int, bytes]:
		parameters = dict(request_data.parameters) if request_data else {}
		method = url.rsplit('/', 1)[-1]
		result = self._result(method, parameters)
		self.calls.append(
			TelegramCall(method=method, parameters=parameters, result=result),
		)
		return 200, json.dumps({'ok': True, 'result': result}).encode()

	def _result(
		self, method: str, parameters: dict[str, Any],
	) -> dict[str, Any]:
		if method == 'getMe':
			# `Bot.initialize()` проверяет токен через getMe; без валидного User
			# он падает с InvalidToken.
			return _BOT_USER
		if method == 'sendMessage':
			return self._sent_message(parameters)
		raise AssertionError(
			f'заглушка транспорта не обслуживает метод {method}: {parameters}',
		)

	def _sent_message(self, parameters: dict[str, Any]) -> dict[str, Any]:
		# Настоящий API возвращает доставленное сообщение с новым message_id;
		# уведомитель опирается на него, регистрируя пару в реестре (карточка №1).
		message_id = self._next_message_id
		self._next_message_id += 1
		return {
			'message_id': message_id,
			'date': _ANSWER_DATE,
			'chat': {'id': _chat_id(parameters['chat_id']), 'type': 'private'},
			'text': parameters.get('text', ''),
		}


def offline_telegram_provider(transport: OfflineTelegramTransport) -> Provider:
	"""Dishka-провайдер, отдающий настоящий `Bot`, говорящий через заглушку."""

	class _OfflineTelegramProvider(Provider):
		@provide(scope=Scope.APP)
		async def get_telegram_bot(
			self, settings: ProjectSettings,
		) -> AsyncIterator[Bot]:
			bot = Bot(
				token=settings.TG_BOT_TOKEN,
				request=transport,
				get_updates_request=transport,
			)
			await bot.initialize()
			try:
				yield bot
			finally:
				await bot.shutdown()

	return _OfflineTelegramProvider()