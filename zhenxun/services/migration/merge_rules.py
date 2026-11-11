"""核心迁移合并的业务键、隐式引用和预检规则"""

import json
from pathlib import Path

from . import data

KEYS = {
    "PluginInfo": ("module_path",),
    "TaskInfo": ("module",),
    "BotConsole": ("bot_id",),
    "UserConsole": ("user_id",),
    "SignUser": ("user_id",),
    "UserProps": ("user_id",),
    "FriendUser": ("user_id",),
    "GroupConsole": ("platform", "group_id", "channel_id"),
    "GroupInfoUser": ("platform", "group_id", "user_id"),
    "GroupPluginSetting": (
        "bot_id",
        "platform_scope",
        "group_id",
        "channel_id",
        "plugin_name",
    ),
    "BotGroupMembership": ("bot_id", "platform_scope", "group_id", "channel_id"),
    "BotGroupPluginPolicy": ("bot_id", "platform_scope", "group_id", "channel_id"),
    "PluginPolicy": ("name",),
    "BotPluginPolicyBinding": ("bot_id",),
    "PluginLimit": ("plugin_id", "limit_type", "watch_type", "check_type"),
    "BanConsole": ("user_id", "group_id"),
    "GoodsInfo": ("goods_name",),
    "GroupTag": ("name",),
    "GroupTagLink": ("tag_id", "group_id"),
    "AIKnowledgeBase": ("name",),
    "AIKnowledgeDoc": ("kb_id", "filename"),
    "AIKnowledgeChunk": ("doc_id", "seq"),
    "AIPersona": ("name",),
    "AIChatGroup": ("group_id",),
    "AIMemoryCursor": ("session_id",),
    "AISticker": ("md5",),
    "AIStickerBlock": ("md5",),
    "AIModelStat": ("hour", "model", "task"),
    "AITokenUsage": ("user_id", "day"),
    "AIAgentRun": ("run_id",),
    "BusinessIdentityLink": (
        "domain",
        "app_id",
        "scene",
        "group_scope",
        "subject_digest",
    ),
    "BusinessDailyClaim": ("identity_id", "day"),
}


HISTORY = {
    "ChatHistory",
    "Statistics",
    "SignLog",
    "UserGoldLog",
    "UserPropsLog",
    "BotConnectLog",
    "EventLog",
    "FgRequest",
    "ScheduledJob",
    "AIMemory",
    "AIChatMessage",
    "AIAgentCall",
    "AccountBindingChange",
}


REFERENCES = {
    "AIKnowledgeDoc": {"kb_id": "AIKnowledgeBase"},
    "AIKnowledgeChunk": {"kb_id": "AIKnowledgeBase", "doc_id": "AIKnowledgeDoc"},
    "AIMemoryCursor": {"message_id": "AIChatMessage"},
    "AccountBindingChange": {
        "previous_account_id": "UserConsole",
        "account_id": "UserConsole",
    },
    "BusinessDailyClaim": {"account_id": "UserConsole"},
}
IMPLICIT_FIELDS = {
    "AIPersona": {"knowledge_bases", "subagents"},
    "AIChatGroup": {"knowledge_bases"},
    "GroupTag": {"owner_id", "bot_id"},
    "ScheduledJob": {"target_type", "target_identifier"},
    "GoodsInfo": {"uuid"},
    "UserConsole": {"uid", "props", "platform"},
    "UserPropsLog": {"uuid"},
    "BusinessIdentityLink": {"account_id", "original_account_id", "principal_id"},
    "AIAgentRun": {"source", "session_id", "user_id"},
    "AIAgentCall": {"run_id"},
}


def _rows(path):
    with path.open(encoding="utf-8") as file:
        for line in file:
            yield json.loads(line)


def _has_knowledge(value):
    if isinstance(value, list):
        return any(_has_knowledge(item) for item in value)
    if isinstance(value, dict):
        return bool(value.get("knowledge_bases")) or any(
            _has_knowledge(item) for item in value.values()
        )
    return False


def validate_rules(models=None):
    """校验明确规则与实际模型的数据库字段一致"""
    checked = []
    for model in models if models is not None else data._models():
        name = model.__name__
        if name not in KEYS.keys() | HISTORY:
            continue
        fields = set(model._meta.fields_db_projection)
        required = (
            set(KEYS.get(name, ()))
            | set(REFERENCES.get(name, ()))
            | IMPLICIT_FIELDS.get(name, set())
        )
        missing = required - fields
        if missing:
            raise data.DataError(
                f"migration_merge_rule_fields_missing:{name}:"
                + ",".join(sorted(missing))
            )
        checked.append(name)
    return checked


def validate_package(directory: Path, included_tables=None, excluded_tables=None):
    """核对所选表是否具有明确的合并规则"""
    manifest = json.loads((directory / data.MANIFEST).read_text(encoding="utf-8"))
    models = {data._table_key(m): m for m in data._models()}
    validate_rules(models.values())
    unsupported = []
    for table, spec in manifest["tables"].items():
        if table in (excluded_tables or ()) or (
            included_tables is not None and table not in included_tables
        ):
            continue
        model = models.get(table)
        if spec["rows"] and (
            model is None or model.__name__ not in KEYS.keys() | HISTORY
        ):
            unsupported.append(table)
    if unsupported:
        raise data.DataError(
            "migration_merge_unsupported_tables:" + ",".join(sorted(unsupported))
        )
    selected = {
        model.__name__: model
        for table, model in models.items()
        if table in manifest["tables"]
        and table not in (excluded_tables or ())
        and (included_tables is None or table in included_tables)
    }
    visited, visiting = set(), set()

    def visit(name):
        if name in visited:
            return
        if name in visiting:
            raise data.DataError("migration_merge_reference_cycle:" + name)
        visiting.add(name)
        model = selected[name]
        if manifest["tables"][data._table_key(model)]["rows"]:
            parents = set(REFERENCES.get(name, {}).values())
            for field in model._meta.fk_fields | model._meta.o2o_fields:
                parents.add(model._meta.fields_map[field].related_model.__name__)
            if name in {
                "AIPersona",
                "AIChatGroup",
                "ScheduledJob",
                "UserConsole",
                "AIAgentCall",
            }:
                spec = manifest["tables"][data._table_key(model)]
                for values in _rows(directory / f"{data._table_key(model)}.jsonl"):
                    row = dict(zip(spec["fields"], values, strict=True))
                    if row.get("knowledge_bases") or _has_knowledge(
                        row.get("subagents")
                    ):
                        parents.add("AIKnowledgeBase")
                    if row.get("target_type") == "TAG":
                        parents.add("GroupTag")
                    if name == "UserConsole" and row.get("props"):
                        parents.add("GoodsInfo")
                    if name == "AIAgentCall" and row.get("run_id"):
                        parents.add("AIAgentRun")
            for parent in sorted(parents):
                if parent not in selected:
                    raise data.DataError(
                        f"migration_merge_required_parent:{name}->{parent}"
                    )
                visit(parent)
        visiting.remove(name)
        visited.add(name)

    for name in selected:
        visit(name)
    return {"unsupported_tables": unsupported}
