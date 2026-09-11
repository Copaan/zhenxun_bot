from collections.abc import Callable

from pydantic import BaseModel

# 数据库操作超时设置（秒）
DB_TIMEOUT_SECONDS = 3.0

# 启动期自动补齐字段/索引可能需要等待数据库锁或扫描较大的表，单独放宽超时
DB_SCHEMA_GUARD_TIMEOUT_SECONDS = 30.0

# 性能监控阈值（秒）
SLOW_QUERY_THRESHOLD = 0.5

LOG_COMMAND = "DbContext"


POSTGRESQL_CONFIG = {
    "maxsize": 30,  # 最大连接数
    "minsize": 5,  # 最小保持的连接数（可选）
}


MYSQL_CONFIG = {
    "maxsize": 20,  # 最大连接数
    "connect_timeout": 30,  # 连接超时（可选）
}

SQLITE_CONFIG = {
    "journal_mode": "WAL",  # 提高并发写入性能
    # SQLite 底层锁等待不应超过上层 DB_TIMEOUT_SECONDS=3.0。
    # Windows bind mount / Docker 场景下 SQLite 不适合高写并发。
    "busy_timeout": 3000,
    "foreign_keys": "ON",
}


class DbModel(BaseModel):
    script_method: list[tuple[str, Callable]] = []
    models: list[str] = []


db_model = DbModel()


prompt = """
**********************************************************************
🌟 **************************** 配置为空 ************************* 🌟
🚀 WebUI 启动完成后，请打开控制台输出的连接链接进行基础配置 🚀
***********************************************************************
***********************************************************************
"""
