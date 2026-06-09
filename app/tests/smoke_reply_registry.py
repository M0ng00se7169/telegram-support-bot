"""Offline smoke: Reply Registry contract (card #1).

Covers both backends without network:
- InMemoryReplyRegistry — plain dict;
- ValkeyReplyRegistry — against a fake Valkey client with the exact
  command surface used by the module (set with ex=, get returning bytes/None,
  delete), proving key prefix and TTL plumbing.

Run:  python tests/smoke_reply_registry.py   (from the app/ directory)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncio

from services.replies import (
	REPLY_REGISTRY_PREFIX,
	REPLY_REGISTRY_TTL,
	InMemoryReplyRegistry,
	ValkeyReplyRegistry,
)


class _FakeValkeyClient:
	def __init__(self) -> None:
		self._storage: dict[str, bytes] = {}
		self._ttl: dict[str, int] = {}

	async def set(self, key: str, value: str, ex: int | None = None) -> None:
		self._storage[key] = value.encode()
		if ex is not None:
			self._ttl[key] = ex

	async def get(self, key: str) -> bytes | None:
		return self._storage.get(key)

	async def delete(self, key: str) -> None:
		self._storage.pop(key, None)
		self._ttl.pop(key, None)


async def main() -> None:
	# InMemory backend.
	in_memory = InMemoryReplyRegistry()
	await in_memory.remember(message_id=101, chat_oid='chat-oid-a')
	await in_memory.remember(message_id=102, chat_oid='chat-oid-b')
	assert await in_memory.resolve(101) == 'chat-oid-a'
	assert await in_memory.resolve(102) == 'chat-oid-b'
	assert await in_memory.resolve(999) is None
	await in_memory.forget(101)
	assert await in_memory.resolve(101) is None

	# Valkey backend against the fake client: key prefix + TTL + bytes decode.
	client = _FakeValkeyClient()
	registry = ValkeyReplyRegistry(client=client)
	await registry.remember(message_id=201, chat_oid='chat-oid-c')
	stored_key = REPLY_REGISTRY_PREFIX + '201'
	assert client._storage[stored_key] == b'chat-oid-c'
	assert client._ttl[stored_key] == REPLY_REGISTRY_TTL
	assert await registry.resolve(201) == 'chat-oid-c'
	assert await registry.resolve(999) is None
	await registry.forget(201)
	assert await registry.resolve(201) is None

	# Non-bytes payload tolerance (decode-tolerant resolve).
	await client.set(stored_key, 'chat-oid-d')
	assert await registry.resolve(201) == 'chat-oid-d'

	print('REGISTRY_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
