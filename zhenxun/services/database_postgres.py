"""Tortoise adapter passing resolved DSN options to its existing asyncpg pool."""

import asyncpg
from tortoise.backends.asyncpg.client import AsyncpgDBClient


class ResolvedAsyncpgClient(AsyncpgDBClient):
    async def create_pool(self, **kwargs):
        """Preserve asyncpg's SSL negotiation and certificate option semantics."""
        try:
            return await asyncpg.create_pool(kwargs.pop("dsn", None), **kwargs)
        finally:
            self._template.pop("dsn", None)


client_class = ResolvedAsyncpgClient
