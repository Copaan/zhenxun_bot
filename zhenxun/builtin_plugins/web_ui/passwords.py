"""Compatibility exports for the database-independent password implementation."""

from zhenxun.utils.passwords import (
    hash_password,
    is_password_hash,
    validate_new_password,
    verify_password,
)

__all__ = [
    "hash_password",
    "is_password_hash",
    "validate_new_password",
    "verify_password",
]
