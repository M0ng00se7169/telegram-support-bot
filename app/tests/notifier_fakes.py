from collections.abc import Sequence
from dataclasses import (
	dataclass,
	field,
)

from dishka import (
	provide,
	Provider,
	Scope,
)

from dtos.messages import ChatListItemDTO
from services.notifier import (
	BaseNotifier,
	NewMessageNotification,
	TelegramAddress,
)


@dataclass(frozen=True)
class Acknowledgement:
	tg_chat_id: TelegramAddress
	text: str
	reply_to_message_id: int | None


@dataclass
class RecordingNotifier(BaseNotifier):
	notifications: list[tuple[TelegramAddress, NewMessageNotification]] = field(
		default_factory=list,
	)
	acknowledgements: list[Acknowledgement] = field(default_factory=list)

	async def notify_new_message(
		self,
		tg_chat_id: TelegramAddress,
		notification: NewMessageNotification,
	) -> None:
		self.notifications.append((tg_chat_id, notification))

	async def notify_chat_list(
		self,
		tg_chat_id: TelegramAddress,
		chats: Sequence[ChatListItemDTO],
	) -> None:
		raise AssertionError('chat list notification is outside this smoke')

	async def acknowledge(
		self,
		tg_chat_id: TelegramAddress,
		text: str,
		reply_to_message_id: int | None = None,
	) -> None:
		self.acknowledgements.append(
			Acknowledgement(tg_chat_id, text, reply_to_message_id),
		)


def assert_single_notification(
	notifier: RecordingNotifier,
	*,
	expected_tg_chat_id: TelegramAddress,
	expected_notification: NewMessageNotification,
	context: str,
) -> None:
	assert (
		len(notifier.notifications) == 1
	), f'{context}: expected 1 notification, got {notifier.notifications!r}'
	tg_chat_id, notification = notifier.notifications[0]
	assert (
		tg_chat_id == expected_tg_chat_id
	), f'{context}: expected recipient {expected_tg_chat_id!r}, got {tg_chat_id!r}'
	assert notification.chat_oid == expected_notification.chat_oid, (
		f'{context}: expected chat_oid {expected_notification.chat_oid!r}, '
		f'got {notification.chat_oid!r}'
	)
	assert notification.title == expected_notification.title, (
		f'{context}: expected title {expected_notification.title!r}, '
		f'got {notification.title!r}'
	)
	assert notification.text == expected_notification.text, (
		f'{context}: expected text {expected_notification.text!r}, '
		f'got {notification.text!r}'
	)


def notifier_override_provider(notifier: BaseNotifier) -> Provider:
	class NotifierOverrideProvider(Provider):
		@provide(scope=Scope.APP)
		def get_notifier(self) -> BaseNotifier:
			return notifier

	return NotifierOverrideProvider()
