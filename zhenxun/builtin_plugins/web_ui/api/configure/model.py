from typing import Literal

from pydantic import BaseModel, Field


class ClaimRequest(BaseModel):
    code: str = Field(min_length=8, max_length=8)


class DatabaseConfig(BaseModel):
    mode: Literal["sqlite", "mysql", "postgres", "url"] = "sqlite"
    path: str = "data/db/zhenxun.db"
    host: str = "127.0.0.1"
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str = ""
    password: str = ""
    database: str = ""
    url: str = ""


class CacheConfig(BaseModel):
    mode: Literal["NONE", "MEMORY", "REDIS"] = "MEMORY"
    host: str = "127.0.0.1"
    port: int = Field(default=6379, ge=1, le=65535)
    password: str = ""


class NetworkConfig(BaseModel):
    mode: Literal["local", "lan", "custom"] = "lan"
    host: str = ""
    port: int = Field(default=8080, ge=1, le=65535)


class DatabaseProbeRequest(BaseModel):
    database: DatabaseConfig


class CacheProbeRequest(BaseModel):
    cache: CacheConfig


class NetworkProbeRequest(BaseModel):
    network: NetworkConfig


class ProbeResult(BaseModel):
    status: Literal["ok", "warning", "error"]
    code: str
    message: str
    latency_ms: int
    facts: dict[str, str | int | bool | list[str]] = Field(default_factory=dict)


class ApplyRequest(BaseModel):
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=8, max_length=1024)
    confirm_password: str = Field(min_length=8, max_length=1024)
    superusers: list[str] = Field(default_factory=list)
    database: DatabaseConfig
    cache: CacheConfig = Field(default_factory=CacheConfig)
    network: NetworkConfig = Field(default_factory=NetworkConfig)
    accept_warnings: bool = False


class RestartRequest(BaseModel):
    receipt: str = Field(min_length=16, max_length=256)


# Compatibility payloads for old WebUI builds.
class DatabaseTest(BaseModel):
    db_url: str


class RedisTest(BaseModel):
    redis_host: str
    redis_port: int = Field(default=6379, ge=1, le=65535)
    redis_password: str = ""


class Setting(BaseModel):
    superusers: list[str]
    db_url: str
    host: str
    port: int = Field(ge=1, le=65535)
    username: str
    password: str
    cache_mode: Literal["NONE", "MEMORY", "REDIS"] = "MEMORY"
    redis_host: str = "127.0.0.1"
    redis_port: int = Field(default=6379, ge=1, le=65535)
    redis_password: str = ""
