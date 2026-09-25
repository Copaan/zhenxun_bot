from __future__ import annotations


class MigrationError(RuntimeError):
    """Only stable codes and relative paths may cross the public API boundary."""

    def __init__(
        self,
        code: str,
        *,
        path: str | None = None,
        status: int = 400,
        details: dict | None = None,
    ):
        self.code = code
        self.path = path
        self.status = status
        self.details = details or {}
        super().__init__(code)

    def public(self) -> dict:
        return {
            "code": self.code,
            **({"path": self.path} if self.path else {}),
            **self.details,
        }
