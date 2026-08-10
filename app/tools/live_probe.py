import asyncio
import sys
import os
import json
import urllib.request

URL = os.environ.get('ROUTE_RELAY_URL', 'http://localhost:8000')
KAFKA_BROKER = os.environ.get('KAFKA_BROKER_URL', 'localhost:29092')
VALKEY_URL = os.environ.get('VALKEY_URL', 'valkey://localhost:6379/0')
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '123456:TEST-TOKEN')

print(f'ROUTE_RELAY_URL={URL}', file=sys.stderr)
print(f'KAFKA_BROKER_URL={KAFKA_BROKER}', file=sys.stderr)
print(f'VALKEY_URL={VALKEY_URL}', file=sys.stderr)
print(f'BOT_TOKEN={BOT_TOKEN!r}', file=sys.stderr)

CHAT_OID = 'chat-oid-77'


async def probe() -> None:
    print('\n--- RouteRelay: openapi inventory ---', file=sys.stderr)

    req = urllib.request.Request(f'{URL}/openapi.json')
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            d = json.loads(resp.read())
            for path, methods in d.get('paths', {}).items():
                for m, op in methods.items():
                    print(f'{m.upper()} {path} - {op.get("summary","")}', file=sys.stderr)
    except Exception as e:
        print(f'openapi inventory: {type(e).__name__}: {e}', file=sys.stderr)

    print('\n--- RouteRelay: auth + chats ---', file=sys.stderr)

    def fetch_token():		req = urllib.request.Request(f'{URL}/auth/token', data=b'username=admin&password=admin&grant_type=password', method='POST')
		with urllib.request.urlopen(req, timeout=8) as resp:
			body = json.loads(resp.read())
			token = body.get('access_token') or body.get('token')
			return token

    try:
        token = fetch_token()
        print(f'auth: got token={token!r}', file=sys.stderr)
    except Exception as e:
        print(f'auth: {type(e).__name__}: {e}', file=sys.stderr)
        return

    headers = {'Authorization': f'Bearer {token}'}

    def get(path):
        req = urllib.request.Request(f'{URL}{path}', headers=headers)
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status, resp.read()

    def post(path, body_bytes):
        req = urllib.request.Request(f'{URL}{path}', data=body_bytes, headers=headers, method='POST')
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status, resp.read()

    def print_ok(status, info, body_head=None):
        ok = status == 200
        print(f'{info}: {status} {"OK" if ok else "FAIL"}', file=sys.stderr)
        if body_head:
            print(f'    body: {body_head}', file=sys.stderr)

    status, body = get('/chat/')
    print_ok(status, '/chat/', body[:200])
    chats = json.loads(body)
    print(f'    items count={len(chats.get("items",[]))}', file=sys.stderr)
    for c in chats.get('items', [])[:2]:
        print(f'    sample: oid={c.get("oid")} title={c.get("title")}', file=sys.stderr)

    status, body = get(f'/chat/{CHAT_OID}/')
    print_ok(status, f'/chat/{CHAT_OID}/', body[:200])

    status, body = get(f'/chat/{CHAT_OID}/listeners/')
    print_ok(status, f'/chat/{CHAT_OID}/listeners/', body[:200])

    status, body = post(f'/chat/{CHAT_OID}/messages/', json.dumps({'text': 'live probe 9'}).encode())
    print_ok(status, f'/chat/{CHAT_OID}/messages/', body[:200])
    if status == 200:
        msg = json.loads(body)
        print(f'    new_message: {msg}', file=sys.stderr)

    print('\n--- Kafka reachable (consumer ping) ---', file=sys.stderr)
    try:
        from aiokafka import AIOKafkaConsumer
        consumer = AIOKafkaConsumer(
            'new-messages',
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            group_id='probe',
            request_timeout_ms=3000,
            session_timeout_ms=8000,
            heartbeat_interval_ms=2000,
        )
        await consumer.start()
        member = consumer._client.cluster.members()
        print(f'kafka: consumer started, cluster members: {len(member)}', file=sys.stderr)
        try:
            async with asyncio.timeout(5):
                async for msg in consumer:
                    print(f'kafka consumed: topic={msg.topic} partition={msg.partition} offset={msg.offset} payload={msg.value.decode()!r}', file=sys.stderr)
                    break
        except TimeoutError:
            print('kafka: no messages on new-messages within timeout (expected if no producer)', file=sys.stderr)
        await consumer.stop()
    except Exception as e:
        print(f'kafka: {type(e).__name__}: {e}', file=sys.stderr)

    print('\n--- Valkey reachable (PING) ---', file=sys.stderr)
    try:
        from valkey.asyncio import ConnectionPool
        pool = ConnectionPool.from_url(VALKEY_URL)
        conn = await pool.get_connection('PING')
        resp = await conn.execute_command('PING')
        print(f'valkey: PING -> {resp!r}', file=sys.stderr)
        await conn.close()
    except Exception as e:
        print(f'valkey: {type(e).__name__}: {e}', file=sys.stderr)

    print('\n--- Bot lifecycle (real token, no network send expected) ---', file=sys.stderr)
    try:
        import telegram
        bot = telegram.Bot(token=BOT_TOKEN)
        await bot.initialize()
        print('bot: initialize OK', file=sys.stderr)
        me = await bot.get_me()
        print(f'bot: get_me -> {me.username} ({me.id})', file=sys.stderr)
        await bot.shutdown()
        print('bot: shutdown OK', file=sys.stderr)
    except telegram.error.InvalidToken as e:
        print(f'bot: InvalidToken - {e}', file=sys.stderr)
    except Exception as e:
        print(f'bot: {type(e).__name__}: {e}', file=sys.stderr)


def main() -> None:
    asyncio.run(probe())


if __name__ == '__main__':
    main()
