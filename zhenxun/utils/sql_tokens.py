"""SQLite 建表语句的无损切分与唯一约束替换"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import re


@dataclass(frozen=True)
class Token:
    kind: str
    text: str
    start: int
    end: int


def tokens(sql: str) -> Iterator[Token]:
    """按 SQLite 词法切分 SQL，跳过空白与注释

    参数:
        sql: SQL 语句

    返回:
        Iterator[Token]: 词元
    """
    index = 0
    while index < len(sql):
        start = index
        character = sql[index]
        if character.isspace():
            index += 1
            continue
        if sql.startswith("--", index):
            end = sql.find("\n", index)
            index = len(sql) if end < 0 else end + 1
            continue
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            if end < 0:
                raise ValueError("sqlite_sql_invalid")
            index = end + 2
            continue
        if character in "'\"`[":
            closing = "]" if character == "[" else character
            index += 1
            while index < len(sql):
                if sql[index] == closing:
                    if index + 1 < len(sql) and sql[index + 1] == closing:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            else:
                raise ValueError("sqlite_sql_invalid")
            kind = "string" if character == "'" else "identifier"
            yield Token(kind, sql[start:index], start, index)
            continue
        match = re.match(r"[A-Za-z_][A-Za-z_0-9$]*", sql[index:])
        if match:
            index += len(match[0])
            yield Token("word", match[0], start, index)
        else:
            index += 1
            yield Token("symbol", character, start, index)


def replace_sqlite_unique(
    sql: str,
    temporary: str,
    obsolete: set[tuple[str, ...]],
    desired: tuple[str, ...],
    constraint: str,
) -> str:
    """改写建表语句：去掉旧唯一约束，加上新的命名唯一约束，其余定义原样保留

    参数:
        sql: 原表的 CREATE TABLE 语句
        temporary: 新表名
        obsolete: 需要去掉的唯一约束列组合
        desired: 新唯一约束的列
        constraint: 新唯一约束名

    返回:
        str: 新表的 CREATE TABLE 语句
    """

    def quote(value):
        return '"' + value.replace('"', '""') + '"'

    parsed = list(tokens(sql))
    opening = next(index for index, token in enumerate(parsed) if token.text == "(")
    depth, start, segments = 0, parsed[opening].end, []
    closing = None
    for token in parsed[opening:]:
        if token.text == "(":
            depth += 1
        elif token.text == ")":
            depth -= 1
            if depth == 0:
                segments.append(sql[start : token.start])
                closing = token.start
                break
        elif token.text == "," and depth == 1:
            segments.append(sql[start : token.start])
            start = token.end
    if closing is None:
        raise RuntimeError("sqlite_schema_unconfirmed")
    retained = []
    for segment in segments:
        fields = list(tokens(segment))
        position = 2 if fields and fields[0].text.upper() == "CONSTRAINT" else 0
        if position < len(fields) and fields[position].text.upper() == "UNIQUE":
            names = []
            first = True
            for token in fields[position + 2 :]:
                if token.text == ")":
                    break
                if token.text == ",":
                    first = True
                elif first:
                    names.append(token.text.strip('`"[]'))
                    first = False
            if tuple(names) in {*obsolete, desired}:
                continue
        retained.append(segment)
    retained.append(
        f"CONSTRAINT {quote(constraint)} UNIQUE ({','.join(map(quote, desired))})"
    )
    return f"CREATE TABLE {quote(temporary)} (" + ",".join(retained) + sql[closing:]
