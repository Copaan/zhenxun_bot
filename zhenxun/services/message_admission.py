"""Connection-local message timestamps, independent of plugin permissions."""

from dataclasses import dataclass
import math
import time
import uuid
import weakref


@dataclass(frozen=True)
class ConnectionEpoch:
    bot: weakref.ReferenceType
    connected_second: int
    connection_id: str


class ConnectionEpochs:
    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], ConnectionEpoch] = {}

    def connect(self, bot, scope: str) -> ConnectionEpoch:
        key = (scope, str(bot.self_id))

        def expired(reference):
            current = self._entries.get(key)
            if current is not None and current.bot is reference:
                self._entries.pop(key, None)

        epoch = ConnectionEpoch(
            weakref.ref(bot, expired), math.floor(time.time()), uuid.uuid4().hex
        )
        self._entries[key] = epoch
        return epoch

    def get(self, bot, scope: str) -> ConnectionEpoch | None:
        epoch = self._entries.get((scope, str(bot.self_id)))
        return epoch if epoch is not None and epoch.bot() is bot else None

    def disconnect(self, bot, scope: str) -> None:
        if self.get(bot, scope) is not None:
            self._entries.pop((scope, str(bot.self_id)), None)

    def is_backlog(self, bot, scope: str, timestamp: object) -> bool:
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, int | float)
            or (isinstance(timestamp, float) and not math.isfinite(timestamp))
            or timestamp < 0
        ):
            return False
        epoch = self.get(bot, scope)
        return epoch is not None and timestamp < epoch.connected_second

    def clear(self) -> None:
        self._entries.clear()


connection_epochs = ConnectionEpochs()
