# RouteRelay Telegram Bridge

A support desk that lives inside Telegram.

This repository is the **Telegram side** of [RouteRelay](#glossary): a pair of
Python processes that let support managers answer customer questions from a
chat app instead of a web console — and that keep working when the message
stream goes sideways.

- **Bot process** — long-polls Telegram, shows managers which support chats are
  open, and accepts a manager's *reply* for the customer's conversation.
- **Consumer process** — subscribes to RouteRelay's Kafka event stream and
  pushes a Telegram notification every time a customer writes in.

They are separate processes on purpose (see [Design](#design-philosophy)): one
can be restarted, scaled or moved without the other even noticing.

---

## What it does, in one slide

```
customer types on the web  ──►  RouteRelay  ──►  Kafka  ──►  consumer  ──►  Telegram
                                                                              │
                                                                         manager replies
                                                                              │
customer sees the answer    ◄──  RouteRelay  ◄──  bot  ◄──  reply handler ◄──┘
```

A manager never copies a chat ID by hand, and a customer never learns what
Kafka is. The bridge owns the translation between a `chat_oid` (the web app's
identity for a conversation) and a Telegram message.

> The bridge currently notifies, registers replies and routes every bot
> message through one module. Handing a manager's reply back to the web app is
> the last hop being finished — see [Status](#status).

---

## The lifecycle of one message

1. A customer posts a message in the RouteRelay web app.
2. RouteRelay writes a `New-Message Event` to the `new-messages` Kafka topic
   and publishes chat lifecycle events to `new-chats-topic` /
   `chat-deleted-topic`.
3. The consumer validates the raw payload at the **process boundary**, then —
   and only then — does anything else.
4. An **Event Deduplicator** claims the event by its `event_id`. If the same
   event arrives twice, the second copy leaves zero side effects.
5. The **Chat Title Cache** resolves the conversation title with *zero* HTTP
   requests in the warm case. A deleted chat is dropped here, before any
   network call.
6. The consumer fetches the chat's listeners (one HTTP request) and asks the
   **Notifier** to send each of them a message through Telegram.
7. The Notifier renders and escapes the message, sends it, and records the
   `(telegram_message_id → chat_oid)` pair in the **Reply Registry**.
8. A manager replies to that Telegram notification.
9. The bot's reply handler resolves the pair back to a `chat_oid` and accepts
   the reply for the web app (final delivery to the customer is the next card).

Every arrow above has an offline smoke test — none of this needs a network to
prove it works.

---

## Two processes, one repository

| Process | Entry point | Talks to |
| --- | --- | --- |
| **Bot** | `main.py` (`python-telegram-bot`, long polling) | Telegram, Web API, Valkey |
| **Consumer** | `consumers/main.py` (`FastStream` + Kafka) | Kafka, Web API, Valkey, Telegram |

They share only two things, both deliberately external:

- **Valkey** — the Reply Registry and the Event Deduplicator. Shared memory
  would be a lie across processes, so shared *state* is a real service.
- **The Web API** — the single source of truth for chats and listeners.

The **Chat Title Cache**, by contrast, is deliberately process-local: only the
consumer needs it, so giving it a shared backend would create a seam with no
second user.

---

## Core modules

The code is organised as **deep modules**: small interfaces in front, real work
behind them. Consumers of these modules never see a status code, a `dict`, a
`parse_mode`, or a raw payload.

| Module | Responsibility | Hides |
| --- | --- | --- |
| `services/web.py` — `ChatDirectory` | Chat list, chat info, listeners, subscribe | httpx, URLs, response parsing, timeouts, `WebApiError` |
| `services/notifier.py` — `Notifier` | The **only** place that sends to Telegram | `Bot`, `parse_mode`, MarkdownV2/HTML escaping, Reply Registry writes |
| `services/replies.py` — `ReplyRegistry` | `telegram_message_id → chat_oid` | Valkey keys and TTL |
| `services/dedup.py` — `EventDeduplicator` | Atomic "was this event seen?" | `SET … EX … NX`, key prefix, TTL |
| `services/title_cache.py` — `ChatTitleCache` | Chat titles without an N+1 | warm-up, read-through, tombstones |
| `consumers/schemas.py` | Wire-format mirrors of the server events | exact field names, types and optionality |
| `exceptions/web.py` — `WebApiError` | One error type for the whole Web API boundary | which endpoint, which status, what detail |

`Notifier` is the clearest example: it exposes `notify_new_message`,
`notify_chat_list` and `acknowledge` — business operations — and nothing about
Telegram leaks out. Replacing Telegram with something else would touch exactly
one file.

---

## The event contract (current work: `card-5`)

The consumer mirrors RouteRelay's server-side `BaseEvent` field for field. The
wire form is **exactly five fields, in server order**:

```json
{
  "event_id":     "12345678-1234-5678-1234-567812345678",
  "occurred_at":  "2026-10-07T00:00:00",
  "message_text": "hello",
  "message_oid":  "m1",
  "chat_oid":     "chat-oid"
}
```

Rules the consumer lives by:

- **Strict where it matters.** The five required fields are required. An event
  missing one is *broken*, not "partially usable".
- **Tolerant where it can be.** Unknown fields are accepted, so RouteRelay can
  enrich a payload (adding `title`, say) without breaking a deployed consumer.
- **Mirrored, not renamed.** The field is `chat_title` because the server calls
  it `chat_title`. A rename on the server should break the client *loudly*.
- **`occurred_at` is a naive local timestamp** with no timezone. It is never
  treated as UTC and never used for arithmetic.

Lifecycle events use the same discipline: `NewChatCreatedEvent` (with
`chat_title`) and `ChatDeletedEvent` (without it).

---

## Reliability: what happens when things go wrong

The stream is **at-least-once**, and the design assumes it.

- **Duplicate delivery** — the `event_id` is claimed with a single atomic
  `SET … EX … NX`. The claim and the check are the same call, so two concurrent
  copies can't both win. A duplicate does no HTTP, no send, no registry write.
- **Broken payloads** — validated at the boundary, counted, logged with the
  `event_id` and the exact reason, then **swallowed**. A bad row must not stop
  the topic, and an unmailable notification is worse than none.
- **Chat lifecycle** — creation keeps the title cache warm; deletion leaves a
  *tombstone* so notifications for a dead chat are skipped **before** any HTTP
  request. `remember`/`forget` are idempotent, so these need no dedup.
- **Cache misses** — a cold title is fetched once (read-through) and reused.
  If the chat is unavailable, the `chat_oid` is used as a fallback title:
  silence is worse than an imprecise label.
- **Programming bugs are not swallowed.** Only boundary failures (`WebApiError`,
  parse errors) are caught. A `TypeError` inside a service is a bug, and it
  surfaces as one.

---

## Project layout

```
.
├── main.py                    # Bot process: PTB wiring, handlers, conversation state
├── consumers/
│   ├── main.py                # Consumer process: FastStream app + title-cache warm-up
│   ├── handlers.py            # Kafka subscriptions (new-message, chat created/deleted)
│   ├── schemas.py             # Wire-format mirrors of server events
│   └── metrics.py             # broken / duplicate counters
├── containers/
│   ├── providers.py           # Dishka providers: scopes, backends, adapters
│   └── factories.py           # App-scoped container singleton
├── handlers/                  # Bot commands: /start /chats /listen_chat /start_dialog /quit
├── services/                  # Deep modules (web, notifier, replies, dedup, title_cache)
├── dtos/                      # ChatListItemDTO, ChatListenerDTO
├── exceptions/                # ApplicationException, WebApiError
├── tests/                     # Offline smoke suite (see below)
└── tools/                     # live_probe.py — probe a real stand
```

Dependency injection is [Dishka](https://dishka.readthedocs.io/). Scopes are
strict: HTTP clients are `REQUEST`-scoped, long-lived things (the Telegram
`Bot`, Valkey pool, caches) are `APP`-scoped. The container is the only place
that knows how a real backend is wired — tests replace it wholesale.

---

## Getting started

### Requirements

- Python **3.11+**
- A Telegram bot token (from [@BotFather](https://t.me/BotFather))
- Reachable Kafka and Valkey instances for real runs (smoke tests need neither)

There is currently **no dependency manifest committed** (no
`requirements.txt` / `pyproject.toml`). Install the dependencies directly:

```bash
python -m venv .venv
source .venv/Scripts/activate      # Windows (Git Bash); use .venv/bin/activate on Unix

pip install \
  "python-telegram-bot>=21.6" \
  "faststream[cli,kafka]" \
  dishka \
  httpx pydantic pydantic-settings django-environ valkey
```

Dishka ships its FastStream integration in the base package, so no extra is
needed; `faststream[cli]` provides the `faststream` command used to run the consumer.

> Adding a pinned `requirements.txt` or `pyproject.toml` would be a welcome
> first contribution — the imports above are the full third-party surface.

### Configuration

Settings are read from `.env` via [django-environ](https://django-environ.readthedocs.io/)
and validated by pydantic-settings (`settings.py`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `TG_BOT_TOKEN` | *(required)* | Telegram bot token |
| `GREETINGS_TEXT` | built-in text | Reply to `/start` |
| `WEB_API_BASE_URL` | `http://main-app:8000` | RouteRelay Web API base URL |
| `KAFKA_BROKER_URL` | `kafka:29092` | Kafka bootstrap servers |
| `NEW_MESSAGE_TOPIC` | `new-messages` | Incoming customer messages |
| `KAFKA_GROUP_ID` | `tg-bot` | Consumer group for `new-messages` |
| `NEW_CHATS_TOPIC` | `new-chats-topic` | Chat-created lifecycle events |
| `CHAT_DELETED_TOPIC` | `chat-deleted-topic` | Chat-deleted lifecycle events |
| `CHAT_EVENTS_GROUP_ID` | `tg-bot-chat-events` | Third, separate group for lifecycle events |
| `VALKEY_URL` | `valkey://valkey:6379/0` | Reply Registry + deduplicator store |
| `EVENT_DEDUP_TTL` | `86400` (24h) | How long an `event_id` stays claimed |

A minimal `.env`:

```dotenv
TG_BOT_TOKEN=123456:your-token-here
WEB_API_BASE_URL=http://localhost:8000
KAFKA_BROKER_URL=localhost:29092
VALKEY_URL=valkey://localhost:6379/0
```

### Running

```bash
# Bot process (long polling)
python main.py

# Consumer process (get_app is an app factory, so the CLI needs --factory)
faststream run consumers.main:get_app --factory
```

No launcher is committed for the consumer; any wrapper that calls
`consumers.main.get_app()` and runs the returned `FastStream` app works.

On startup the consumer warms the Chat Title Cache with a single request for
the chat list. If RouteRelay is down, the warm-up is logged and skipped — the
subscription still comes up, and titles are fetched lazily.

---

## Testing: the offline smoke suite

The suite is built around a simple idea — **test the black box, not the mocks**.
It brings up the *real* handlers, the *real* Dishka container and the *real*
Kafka router, then replaces only the outermost edge:

- `faststream`'s `TestKafkaBroker` instead of a Kafka cluster,
- `httpx.MockTransport` instead of the Web API,
- `tests/telegram_transport.py` — an `OfflineTelegramTransport` implementing
  PTB's `BaseRequest` — instead of Telegram. A **real** `telegram.Bot` speaks
  through it, so tests observe "what actually went to Telegram" rather than
  "which method was monkey-patched".

Run any of them from the repository root:

```bash
python tests/smoke_event_contract.py      # event contract: wire form, strict/unknown fields, types
python tests/smoke_dedup.py               # deduplicator: atomicity, TTL, backlog, restarts
python tests/smoke_title_cache.py         # warm-up, read-through, tombstone, hot path
python tests/smoke_notifier.py            # escaping, formats, Reply Registry writes
python tests/smoke_chat_directory.py      # DTO mapping, error taxonomy, timeouts
python tests/smoke_reply_flow.py          # event → notification → manager reply
python tests/smoke_reply_registry.py      # registry contract (in-memory + fake Valkey)
python tests/smoke_bot_handlers.py        # bot commands through the Telegram seam
python tests/smoke_composition_root.py    # end-to-end container wiring
```

`smoke_dedup.py` also exercises a real Valkey when one is reachable
(`VALKEY_URL=valkey://localhost:6379/0`); otherwise it prints `VALKEY_SKIP` and
moves on.

None of these require Kafka, Valkey, a network or a valid bot token to run.

### Probing a real stand

`tools/live_probe.py` is a diagnostic script rather than a test. It walks a
running RouteRelay instance — OpenAPI inventory, auth, chats, chat info,
listeners, posting a message — then pings Kafka, Valkey and the bot token, and
prints a status line for each. Point it at a stand with environment variables:

```bash
ROUTE_RELAY_URL=http://localhost:8000 \
KAFKA_BROKER_URL=localhost:29092 \
VALKEY_URL=valkey://localhost:6379/0 \
TG_BOT_TOKEN=123456:TEST-TOKEN \
python tools/live_probe.py
```

---

## Design philosophy

A few decisions show up everywhere and are worth knowing before you read the
code:

- **Deep modules.** Big behaviour behind a narrow interface. Callers say
  *what* they want, never *how* the network wants it.
- **Boundary validation only.** Types are verified once, at the process edge.
  Everything inside trusts its inputs, and bad data never penetrates.
- **Idempotent by construction.** At-least-once delivery is a fact of the
  system, not an edge case to patch later.
- **Seams for testing, not stubs for mocking.** The design puts real
  interfaces (`BaseRequest`, `httpx` transport, Kafka broker) where they can be
  swapped, so tests run production code paths offline.
- **Bugs are not disguised as fallbacks.** Only expected boundary failures are
  caught; anything else is allowed to blow up loudly.

### House style

New-code docstrings and comments are written in **Russian** and carry design
rationale (the *why*, plus references to ADR numbers); early code and test
output strings are in English. Match the file you're editing.

---

## Glossary

| Term | Meaning |
| --- | --- |
| **RouteRelay** | The customer-support web platform this bridge integrates with. |
| **`chat_oid`** | RouteRelay's stable identifier for a support conversation. |
| **Manager reply** | A manager replying to a Telegram notification to answer a chat. |
| **Event** | One record on Kafka, e.g. a new customer message or a chat lifecycle change. |
| **Tombstone** | A cache marker meaning "this chat is gone" — skips a notification entirely. |
| **Reply Registry** | The shared `telegram_message_id → chat_oid` map that makes replies work. |
| **Deep module** | A unit with a simple interface over substantial hidden behaviour. |

---

## Status

The event-contract work (`card-5`) is on the branch of the same name: strict
wire format, idempotent consumption, and a warm title cache. Delivery of a
manager's reply back to the customer's web app is the next card in line.
