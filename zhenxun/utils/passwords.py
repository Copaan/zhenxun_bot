from __future__ import annotations

import base64
import hashlib
import hmac
import secrets

_PREFIX = "scrypt$v1"
_N = 16384
_R = 8
_P = 1
_DKLEN = 32


def validate_new_password(password: str) -> str | None:
    if len(password) < 8:
        return "密码至少需要 8 位。"
    if not any(character.isupper() for character in password):
        return "密码需要包含大写字母。"
    if not any(character.islower() for character in password):
        return "密码需要包含小写字母。"
    if not any(character.isdigit() for character in password):
        return "密码需要包含数字。"
    return None


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=_N,
        r=_R,
        p=_P,
        dklen=_DKLEN,
    )
    return "$".join(
        (
            _PREFIX,
            str(_N),
            str(_R),
            str(_P),
            base64.urlsafe_b64encode(salt).decode("ascii"),
            base64.urlsafe_b64encode(digest).decode("ascii"),
        )
    )


def is_password_hash(value: str) -> bool:
    return value.startswith(f"{_PREFIX}$")


def verify_password(password: str, stored: str) -> bool:
    if not is_password_hash(stored):
        return hmac.compare_digest(password.encode("utf-8"), stored.encode("utf-8"))
    try:
        parts = stored.split("$")
        if len(parts) != 7 or "$".join(parts[:2]) != _PREFIX:
            return False
        work_factor = int(parts[2])
        block_size = int(parts[3])
        parallelism = int(parts[4])
        if (work_factor, block_size, parallelism) != (_N, _R, _P):
            return False
        salt = base64.urlsafe_b64decode(parts[5].encode("ascii"))
        expected = base64.urlsafe_b64decode(parts[6].encode("ascii"))
        if len(salt) != 16 or len(expected) != _DKLEN:
            return False
    except Exception:
        return False
    try:
        actual = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=work_factor,
            r=block_size,
            p=parallelism,
            dklen=len(expected),
        )
    except (TypeError, ValueError):
        return False
    return hmac.compare_digest(actual, expected)


__all__ = [
    "hash_password",
    "is_password_hash",
    "validate_new_password",
    "verify_password",
]
