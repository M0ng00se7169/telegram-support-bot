# GLOSSARY — доменные и архитектурные термины проекта

Доменные термины (уже живут в коде):

- **Chat** — чат поддержки в RouteRelay (web); идентифицируется `chat_oid`, имеет `title`.
- **Chat Listener** — telegram-аккаунт (`telegram_chat_id`), подписанный на уведомления о новых сообщениях чата.
- **New-Message Event** — событие из Kafka-топика `new-messages` (сервер: `BaseEvent` с `event_id`/`occurred_at`); доставка at-least-once.
- **Manager Reply** — ответ менеджера в Telegram на уведомление бота; должен вернуться в чат через RouteRelay.
- **Reply Chain** — цепочка «Manager Reply → CreateMessageCommand RouteRelay» (`POST /chat/{chat_oid}/messages/`), гарантия дохождения reply до чата и подтверждение менеджеру.
- **Identity Marker** — текущая метка `(<chat_oid>)` в тексте уведомления бота, из которой reply-хендлер выводит целевой чат.
- **Reply Registry (карточка №1)** — реестр соответствий «TG-уведомление ↔ chat_oid» на стороне бота (per-chat pending state), устраняющий regex-парсинг `chat_oid` из текста и хрупкие проверки `reply_to_message`. Реализован 2026-10-07 как deep module `services/replies.py`: уведомитель (consumer) после успешной `send_message` пишет `remember(tg_message_id → chat_oid)`, reply-хендлер бота спрашивает `resolve(message_id)`; TTL записи = окно на ответ (1 ч). Бэкенд — Valkey (общие процессы по ADR-0001 не разделяют память): пул `valkey.asyncio.ConnectionPool`, APP-скоуп, в офлайн-смоуках — `InMemoryReplyRegistry`-override. Доставка reply в чат (`POST /chat/{chat_oid}/messages/`) — отдельная карточка; Identity Marker `(<oid>)` из текста уведомления удалён.

Архитектурные термины:

- **Composition Root** — единственное место сборки DI-контейнера на процесс (бот и consumer собирают один общий корень). Решение 2026-10-06: процессы НЕ сливать, контейнер — синглтон на процесс, провайдеры с yield, потребительский скоуп = одно сообщение Kafka / один апдейт бота.
- **Notifier (карточка №3)** — единственная точка отправки сообщений в Telegram в приложении: наружу предоставляет бизнес-операции (уведомить о новом сообщении, показать список чатов, подтвердить действие), внутри скрывает `Bot`, `parse_mode`, форматы и экранирование. После успешной отправки уведомления о новом сообщении записывает в Reply Registry соответствие `telegram message_id → chat_oid`. Notifier не читает Kafka и не является consumer/пайплайном; доставка Manager Reply в RouteRelay остаётся вне его ответственности.
