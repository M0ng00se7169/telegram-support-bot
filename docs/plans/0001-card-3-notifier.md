# План: карточка №3 — «Telegram-уведомитель: один adapter на всю отправку»

- **Источник:** architecture-review-20261006-231753.html, кандидат №3 (бейдж *Worth exploring*)
- **Статус:** план, готов к реализации
- **Дата:** 2026-10-07
- **Связанные ADR:** `docs/adr/0001-keep-bot-and-consumer-as-separate-processes.md`, `docs/adr/0003-chat-directory-single-web-api-boundary.md`, `docs/adr/0004-application-exceptions-are-not-frozen.md`

## Проблема (из ревью)

Каждое место отправки само знает `parse_mode` (HTML в consumer'е, MarkdownV2 в `/chats`, свой текст в reply-флоу), само делает экранирование и формат blockquote и само достаёт `Bot` из контейнера. `title` чатов **не экранируется** — MarkdownV2 падает на спецсимволах. Тест consumer'а сегодня требует Telegram API (смоуки monkeypatch-ят `telegram.Bot`).

## Целевое состояние

Модуль-гейт **`Notifier`** с интерфейсом бизнес-событий («уведомить о новом сообщении», «показать список чатов», «подтвердить действие»). `Bot`, `parse_mode`, экранирование и форматы — внутри модуля. Пары `(tg_message_id → chat_oid)` для Reply Registry (карточка №1) рождаются именно здесь — после успешной отправки.

Интерфейс (черновик, решённый скетч — не контракт копипасты):

```python
class BaseNotifier(ABC):
    async def notify_new_message(self, chat_oid: str, title: str, text: str) -> None: ...
    async def notify_chat_list(self, chats: list[ChatListItemDTO], tg_chat_id: int) -> None: ...
    async def acknowledge(self, tg_chat_id: int, text: str) -> None: ...
```

## Текущее состояние и швы

- Процессы и composition root — по ADR-0001: `Bot` уже есть в APP-скоупе (`containers/providers.py`), менять топологию не нужно.
- HTTP-граница уже глубокая (ADR-0003): `ChatDirectory` отдаёт DTO — Notifier'у остаётся только презентация.
- Тестируемый шов — **DI-контейнер**: override-провайдер в смоуках (паттерн `OfflineProvider` в `app/tests/`), сеть не поднимается.
- Живой шов — RouteRelay на `localhost:8000` (логин/пароль `admin`), Kafka-топик `new-messages`, чат `e2e-real-listener` с listener'ом `863447939`.

## Билеты (tracer bullets, по порядку зависимостей)

### 1. Гейт `Notifier`: интерфейс + HTML-уведомление о новом сообщении

**Blocked by:** нет (может стартовать сразу).

**Что доставляет:** событие из Kafka доходит до Telegram, причём consumer больше не знает про `parse_mode`/blockquote/`Bot`; после отправки пишется запись Reply Registry.

Критерии приёмки:

- [ ] `consumers/handlers.py` не импортирует `telegram.Bot` и не задаёт `parse_mode`; вызывает только `notifier.notify_new_message(...)`
- [ ] `title` и текст сообщения HTML-экранируются — спецсимволы (`<`, `&`, `>`) не ломают отправку
- [ ] `reply_registry.remember(message_id → chat_oid)` вызывается внутри Notifier'а после успешной отправки, TTL не изменился
- [ ] новый смоук: событие → fake-Notifier получает `(chat_oid, title, text)`; отдельно реальная реализация проверяется на `MockTransport`/стабе `Bot` (экранирование + порядок полей)
- [ ] все существующие смоуки зелёные; `ruff check` по изменённым пакетам чист

### 2. Вся отправка бота через `Notifier` + экранирование MarkdownV2

**Blocked by:** 1.

**Что доставляет:** менеджер делает `/chats` с любым `title` (точки, скобки, дефисы) и получает список — без падения парсера; подтверждения (`Reply accepted for chat …`, `You connected to the chat.`) уходят тем же путём.

Критерии приёмки:

- [ ] ни один `context.bot.send_message` в `app/handlers/` не задаёт `parse_mode` и не форматирует текст сам — все вызовы идут через Notifier
- [ ] `title` со спецсимволами не роняет `/chats` (экранирование MarkdownV2 внутри Notifier'а)
- [ ] reply-ack и остальные подтверждения уходят через Notifier (`acknowledge`)
- [ ] смоук bot-handlers зелёный

### 3. Fake-нотификатор в смоуках: тест consumer'а без Telegram API

**Blocked by:** 1, 2.

**Что доставляет:** офлайн-тесты больше не трогают `telegram.Bot` — проверяется поведение, а не мок библиотеки.

Критерии приёмки:

- [ ] `smoke_composition_root.py` и `smoke_reply_flow.py` не monkeypatch-ят `telegram.Bot`: вместо этого override-провайдер `BaseNotifier` (in-memory sink)
- [ ] проверки текста уведомления (blockquote, title, регистр) живут в тесте Notifier'а, а не рассыпаны по смоукам
- [ ] сетевые стабы в смоуках сведены к `MockTransport` для `ChatDirectory`
- [ ] все смоуки зелёные подряд (5/5 и новый)

### 4. Живой e2e-прогон в Docker после рефакторинга

**Blocked by:** 3.

**Что доставляет:** доказательство, что рефакторинг ничего не сломал в реальном контуре (Kafka → RouteRelay → Telegram).

Критерии приёмки:

- [ ] `docker compose -f docker_compose/consumer.yaml|bot.yaml --env-file .env up --build -d` — оба контейнера Up, в логах 0 `ERROR`
- [ ] `POST /chat/{oid}/messages` → уведомление приходит в Telegram (чат `863447939`)
- [ ] `/chats` у менеджера работает; reply на уведомление даёт ack, в Valkey появляется `reply-registry:<message_id> → chat_oid` с TTL
- [ ] инцидентов вида «ошибка замаскирована под FrozenInstanceError» в логах нет (ADR-0004)

## Границы объёма (Out of scope)

- **Второй adapter (не Telegram)** — в ревью честно помечен как маловероятный: seam создаётся ради локальности форматирования и тестов, а не ради замены PTB.
- **Карточка №5** — дедуп по `event_id`, точное зеркало UUID/datetime, enrichment title в payload (cross-repo с RouteRelay).
- **Доставка reply в чат** (`POST /chat/{chat_oid}/messages/`) — отдельная карточка после №3; сейчас reply-флоу только подтверждает менеджеру.
- **Ретраи/бэк-офф Web API** — связаны с идемпотентностью из карточки №5.

## Определение готовности

Пять (плюс новый) смоуков зелёные, `ruff check` по изменённым пакетам чист, живой e2e из билета 4 пройден, `GLOSSARY.md` дополнен термином *Notifier*.
