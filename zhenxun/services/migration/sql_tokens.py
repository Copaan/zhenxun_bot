"""Lossless SQL tokens for changing object ownership without editing literals."""

from __future__ import annotations

from dataclasses import dataclass
import re

from .errors import MigrationError


@dataclass(frozen=True)
class Token:
    kind: str
    text: str
    start: int
    end: int


def tokens(sql: str, *, offset=0, dialect="mysql"):
    index = 0
    while index < len(sql):
        start = index
        character = sql[index]
        if character.isspace():
            index += 1
            continue
        if character == "$" and dialect == "postgres":
            delimiter = re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", sql[index:])
            if delimiter:
                end = sql.find(delimiter[0], index + len(delimiter[0]))
                if end < 0:
                    raise MigrationError("migration_database_sql_invalid")
                index = end + len(delimiter[0])
                yield Token("string", sql[start:index], start + offset, index + offset)
                continue
        if sql.startswith("--", index) or (character == "#" and dialect == "mysql"):
            end = sql.find("\n", index)
            index = len(sql) if end < 0 else end + 1
            continue
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            if end < 0:
                raise MigrationError("migration_database_sql_invalid")
            if sql.startswith("/*!", index):
                body = index + 3
                while body < end and sql[body].isdigit():
                    body += 1
                yield from tokens(sql[body:end], offset=offset + body, dialect=dialect)
            index = end + 2
            continue
        if character in "'\"`[":
            closing = "]" if character == "[" else character
            index += 1
            while index < len(sql):
                if (
                    sql[index] == "\\"
                    and character in "'\""
                    and (
                        dialect == "mysql"
                        or (
                            dialect == "postgres"
                            and start > 0
                            and sql[start - 1] in "eE"
                        )
                    )
                ):
                    index += 2
                    continue
                if sql[index] == closing:
                    if index + 1 < len(sql) and sql[index + 1] == closing:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            else:
                raise MigrationError("migration_database_sql_invalid")
            yield Token(
                "string" if character == "'" else "identifier",
                sql[start:index],
                start + offset,
                index + offset,
            )
            continue
        match = re.match(r"[A-Za-z_][A-Za-z_0-9$]*", sql[index:])
        if match:
            index += len(match[0])
            yield Token("word", match[0], start + offset, index + offset)
        else:
            index += 1
            yield Token("symbol", character, start + offset, index + offset)


def replace_spans(sql, changes):
    for start, end, value in sorted(changes, reverse=True):
        sql = sql[:start] + value + sql[end:]
    return sql


def mysql_sql(
    sql: str, *, source_database=None, target_database=None, disable_events=False
) -> str:
    """Map DEFINER and qualified database identifiers outside SQL literals."""
    parsed = list(tokens(sql))
    declaration = None
    declaring = False
    ddl_on = False
    changes = []
    skip_until = -1
    event = False
    event_status = False
    clause = ""
    for index, token in enumerate(parsed):
        if token.start < skip_until:
            continue
        word = token.text.upper() if token.kind == "word" else ""
        if word == "CREATE":
            declaring, declaration, ddl_on = True, None, False
        elif declaring and word in {
            "TABLE",
            "VIEW",
            "TRIGGER",
            "EVENT",
            "PROCEDURE",
            "FUNCTION",
            "INDEX",
        }:
            declaring, declaration, ddl_on = False, index, word in {"TRIGGER", "INDEX"}
        elif word == "FOR" or token.text == ";":
            ddl_on = False
            if token.text == ";":
                declaring = False
        if word in {
            "SELECT",
            "FROM",
            "WHERE",
            "GROUP",
            "ORDER",
            "HAVING",
            "VALUES",
            "SET",
            "JOIN",
            "ON",
        }:
            clause = word
        if (
            word == "DEFINER"
            and index + 2 < len(parsed)
            and parsed[index + 1].text == "="
        ):
            end = index + 2
            if end + 2 < len(parsed) and parsed[end + 1].text == "@":
                end += 2
            elif parsed[end].text.upper() != "CURRENT_USER":
                raise MigrationError("migration_database_sql_invalid")
            if (
                end + 2 < len(parsed)
                and parsed[end + 1].text == "("
                and parsed[end + 2].text == ")"
            ):
                end += 2
            changes.append((parsed[index + 2].start, parsed[end].end, "CURRENT_USER"))
            skip_until = parsed[end].end
        if source_database and target_database and token.kind in {"identifier", "word"}:
            name = (
                token.text[1:-1].replace("``", "`")
                if token.text.startswith("`")
                else token.text
            )
            if (
                name == source_database
                and index + 2 < len(parsed)
                and parsed[index + 1].text == "."
            ):
                previous = parsed[index - 1].text.upper() if index else ""
                relation = (
                    previous
                    in {
                        "FROM",
                        "JOIN",
                        "UPDATE",
                        "INTO",
                        "TABLE",
                        "TABLES",
                        "VIEW",
                        "REFERENCES",
                        "CALL",
                        "PROCEDURE",
                        "FUNCTION",
                        "TRIGGER",
                        "EVENT",
                        "EXISTS",
                    }
                    or (previous == "," and clause == "FROM")
                    or (previous == "ON" and ddl_on)
                )
                qualified = index + 3 < len(parsed) and parsed[index + 3].text in {
                    ".",
                    "(",
                }
                if relation or qualified:
                    changes.append(
                        (
                            token.start,
                            token.end,
                            "`" + target_database.replace("`", "``") + "`",
                        )
                    )
        if disable_events:
            if word == "EVENT" and index == declaration:
                event, event_status = True, False
            elif event and word in {"ENABLE", "DISABLE"}:
                event_status = True
                if word == "ENABLE":
                    changes.append((token.start, token.end, "DISABLE"))
            elif event and word == "DO":
                if not event_status:
                    changes.append((token.start, token.start, "DISABLE "))
                event = False
    return replace_spans(sql, changes)


def replace_sqlite_unique(sql, temporary, obsolete, desired, constraint):
    def quote(value):
        return '"' + value.replace('"', '""') + '"'

    parsed = list(tokens(sql, dialect="sqlite"))
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
        fields = list(tokens(segment, dialect="sqlite"))
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
            names = tuple(names)
            if names in {*obsolete, desired}:
                continue
        retained.append(segment)
    retained.append(
        f"CONSTRAINT {quote(constraint)} UNIQUE ({','.join(map(quote, desired))})"
    )
    return f"CREATE TABLE {quote(temporary)} (" + ",".join(retained) + sql[closing:]
