"""Single-worker cache-aside fencing with fixed memory and no per-key history."""

import asyncio


class RefillFence:
    def __init__(self, capacity=256):
        self.epoch = 0
        self.revisions = [0] * capacity
        self.locks = [asyncio.Lock() for _ in range(capacity)]
        self.poisoned = [False] * capacity
        self.discarded = 0

    def stripe(self, key):
        return hash(key) % len(self.revisions)

    def token(self, key):
        index = self.stripe(key)
        return self.epoch, index, self.revisions[index]

    def valid(self, token):
        epoch, index, revision = token
        return (
            epoch == self.epoch
            and revision == self.revisions[index]
            and not self.poisoned[index]
        )

    def invalidate(self, key):
        index = self.stripe(key)
        self.revisions[index] += 1
        return index
