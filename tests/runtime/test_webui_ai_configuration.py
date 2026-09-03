from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi import HTTPException
import pytest
from ruamel.yaml import YAML

from zhenxun.utils.pydantic_compat import model_dump


def _valid_ai() -> dict[str, Any]:
    from zhenxun.services.ai.config.models import (
        AgentEngineSettings,
        ClientSettings,
        ContextManagementSettings,
        DebugLogOptions,
        DefaultModelsConfig,
        ProviderSettingsGroup,
        SandboxSettings,
    )

    return {
        "default_models": model_dump(
            DefaultModelsConfig(
                chat="Test/model",
                embedding=None,
                tts=None,
                image=None,
                rerank=None,
            )
        ),
        "client_settings": model_dump(ClientSettings()),
        "debug_log": model_dump(DebugLogOptions()),
        "context_settings": model_dump(ContextManagementSettings()),
        "MODEL_GROUPS": {"fallback": ["Test/model"]},
        "agent_settings": model_dump(AgentEngineSettings()),
        "sandbox": model_dump(SandboxSettings()),
        "provider_settings": model_dump(ProviderSettingsGroup()),
        "PROVIDERS": [
            {
                "name": "Test",
                "api_key": ["secret-one", "secret-two"],
                "api_base": "https://example.invalid/v1",
                "api_type": "openai",
                "timeout": 30,
                "models": [{"model_name": "model"}],
            }
        ],
    }


def _dump_config(path: Path, ai: dict[str, Any]) -> str:
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    with path.open("w", encoding="utf-8") as stream:
        yaml.dump({"OTHER": {"KEEP": True}, "AI": ai}, stream)
    return path.read_text(encoding="utf-8")


def test_secret_slots_preserve_replace_delete_and_add() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration import (
        SecretSlot,
        _apply_secret_slots,
    )

    result = _apply_secret_slots(
        ["first", "second"],
        [
            SecretSlot(existing_index=1),
            SecretSlot(existing_index=0, value="replacement"),
            SecretSlot(value="new-key"),
        ],
    )

    assert result == ["second", "replacement", "new-key"]
    assert _apply_secret_slots(["first"], []) == []


def test_provider_view_never_contains_secret() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration import (
        _provider_view,
    )
    from zhenxun.services.ai.config.models import ProviderConfig

    view = _provider_view(
        ProviderConfig(
            name="Test",
            api_key=["secret-one", "secret-two"],
            api_base="https://example.invalid/v1",
            models=[{"model_name": "model"}],
        )
    )

    rendered = repr(view)
    assert "secret-one" not in rendered
    assert "secret-two" not in rendered
    assert "api_key" not in view
    assert len(view["api_key_slots"]) == 2
    assert view["credential_status"] == "configured"
    assert view["discovery_status"] == "ready"


def test_provider_view_treats_template_key_as_missing() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration import (
        _provider_view,
    )
    from zhenxun.services.ai.config.models import ProviderConfig

    view = _provider_view(
        ProviderConfig(
            name="DeepSeek",
            api_type="deepseek",
            api_key=["YOUR_DEEPSEEK_API_KEY"],
            api_base="https://api.deepseek.com",
            models=[],
        )
    )

    assert view["api_key_slots"] == []
    assert view["credential_status"] == "missing"
    assert view["discovery_status"] == "missing_credentials"


@pytest.mark.asyncio
async def test_default_persona_update_uses_revision_and_hot_applies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration as module

    class FakePersona:
        def __init__(self, **values: Any) -> None:
            self.__dict__.update(values)

        def to_payload(self) -> dict[str, Any]:
            return dict(self.__dict__)

        @classmethod
        def from_payload(cls, values: dict[str, Any]) -> "FakePersona":
            return cls(**values)

    current = FakePersona(
        persona_id="default",
        name="Old",
        prompt="Old prompt",
        style="quiet",
        tone_examples=("old",),
        preset_dialogues=(),
        bound_tools=(),
        tags=(),
        enabled=True,
        source="file",
    )
    saved: list[FakePersona] = []
    fake_module = SimpleNamespace(
        Persona=FakePersona,
        upsert_persona=lambda persona: saved.append(persona) or persona,
    )
    monkeypatch.setattr(module, "_persona_state", lambda: (fake_module, current))
    revision = module._persona_revision(module._persona_view(current))

    result = await module.update_default_persona(
        module.PersonaUpdate(
            expected_revision=revision,
            name="New",
            prompt="New prompt",
            style="natural",
            tone_examples=[" hello ", ""],
            preset_dialogues=["user: hi\nbot: hello"],
            enabled=True,
        )
    )

    assert result.data["apply_mode"] == "hot_reloaded"
    assert result.data["restart_required"] is False
    assert result.data["persona"]["name"] == "New"
    assert result.data["persona"]["tone_examples"] == ["hello"]
    assert saved[0].prompt == "New prompt"


def test_configuration_view_preserves_safe_unknown_fields_without_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration as module

    ai = _valid_ai()
    ai["client_settings"]["proxy"] = "http://127.0.0.1:7890"
    ai["client_settings"]["vendor_token"] = "must-not-leak"
    ai["PROVIDERS"][0]["models"][0]["max_tokens"] = 4096
    ai["PROVIDERS"][0]["models"][0]["access_token"] = "must-not-leak"
    config_path = tmp_path / "config.yaml"
    content = _dump_config(config_path, ai)
    monkeypatch.setattr(module, "_CONFIG_FILE", config_path)

    config = module._validate_full(ai, strict_references=False)
    view = module._configuration_view(
        config, hashlib.sha256(content.encode()).hexdigest()
    )

    assert view["sections"]["advanced"]["client_settings"]["proxy"].endswith(":7890")
    assert "vendor_token" not in view["sections"]["advanced"]["client_settings"]
    assert view["providers"][0]["models"][0]["max_tokens"] == 4096
    assert "access_token" not in view["providers"][0]["models"][0]


@pytest.mark.asyncio
async def test_model_and_section_updates_keep_unsubmitted_unknown_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration as module

    ai = _valid_ai()
    ai["client_settings"]["proxy"] = "http://127.0.0.1:7890"
    ai["PROVIDERS"][0]["models"][0]["vendor_option"] = "keep"
    config_path = tmp_path / "config.yaml"
    content = _dump_config(config_path, ai)
    monkeypatch.setattr(module, "_CONFIG_FILE", config_path)

    async def reload_ok(*, submit_restart: bool = True) -> None:
        assert submit_restart is False
        return None

    monkeypatch.setattr(module, "reload_runtime_config", reload_ok)
    revision = hashlib.sha256(content.encode()).hexdigest()
    model_result = await module.update_models(
        "Test",
        module.ProviderModelsUpdate(
            expected_revision=revision,
            models=[{"model_name": "model", "is_available": True}],
        ),
    )
    section_result = await module.update_section(
        "advanced",
        module.SectionUpdate(
            expected_revision=model_result.data["revision"],
            value={"client_settings": {"timeout": 60}},
        ),
    )
    groups_result = await module.update_section(
        "model_groups",
        module.SectionUpdate(
            expected_revision=section_result.data["revision"],
            value={},
        ),
    )

    persisted = module._load(config_path.read_text(encoding="utf-8"))["AI"]
    assert persisted["PROVIDERS"][0]["models"][0]["vendor_option"] == "keep"
    assert persisted["client_settings"]["proxy"].endswith(":7890")
    assert persisted["MODEL_GROUPS"] == {}
    assert section_result.data["sections"]["advanced"]["client_settings"][
        "proxy"
    ].endswith(":7890")
    assert groups_result.data["sections"]["model_groups"] == {}


def test_full_validation_rejects_cycles_and_missing_models() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration import (
        _validate_full,
    )

    valid = _valid_ai()
    config = _validate_full(valid)
    assert config.default_models.chat == "Test/model"

    cyclic = _valid_ai()
    cyclic["MODEL_GROUPS"] = {"a": ["b"], "b": ["a"]}
    with pytest.raises(HTTPException) as error:
        _validate_full(cyclic)
    assert error.value.detail["issues"][0]["code"] == "model_group_cycle"

    missing = _valid_ai()
    missing["default_models"]["chat"] = "Test/missing"
    with pytest.raises(HTTPException) as error:
        _validate_full(missing)
    assert error.value.detail["issues"][0]["code"] == "default_model_missing"


@pytest.mark.asyncio
async def test_persist_checks_revision_and_rolls_back_reload_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration as module

    config_path = tmp_path / "config.yaml"
    original = _dump_config(config_path, _valid_ai())
    revision = hashlib.sha256(original.encode()).hexdigest()
    monkeypatch.setattr(module, "_CONFIG_FILE", config_path)

    async def reload_ok(*, submit_restart: bool = True) -> None:
        assert submit_restart is False
        return None

    monkeypatch.setattr(module, "reload_runtime_config", reload_ok)

    def update_timeout(ai: dict[str, Any]) -> None:
        ai["PROVIDERS"][0]["timeout"] = 60

    updated_revision, _ = await module._persist(revision, update_timeout)
    assert updated_revision != revision
    assert "timeout: 60" in config_path.read_text(encoding="utf-8")
    assert "OTHER:" in config_path.read_text(encoding="utf-8")

    with pytest.raises(HTTPException) as conflict:
        await module._persist(revision, update_timeout)
    assert conflict.value.status_code == 409

    before_failure = config_path.read_bytes()

    async def reload_failed(*, submit_restart: bool = True) -> None:
        assert submit_restart is False
        raise RuntimeError("reload failed")

    monkeypatch.setattr(module, "reload_runtime_config", reload_failed)
    current_revision = hashlib.sha256(before_failure).hexdigest()
    with pytest.raises(HTTPException) as failed:
        await module._persist(
            current_revision, lambda ai: ai["PROVIDERS"][0].update(timeout=90)
        )
    assert failed.value.detail["code"] == "ai_reload_failed"
    assert config_path.read_bytes() == before_failure


def test_registered_config_ui_schema_is_serializable() -> None:
    from zhenxun.configs.utils.models import ConfigUIModel

    ui = ConfigUIModel(
        label="检测模式",
        component="select",
        options=[{"label": "关闭", "value": "off"}],
        visible_when={"path": "STATUS", "operator": "eq", "value": True},
    )

    data = model_dump(ui, exclude_none=True)
    assert data["component"] == "select"
    assert data["visible_when"]["path"] == "STATUS"


def test_current_ai_config_is_read_case_insensitively_without_duplicate_keys() -> None:
    import zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration as module

    content = module._read()
    data = module._load(content)
    config = module._validate_full(data["AI"], strict_references=False)
    candidate = dict(data["AI"])
    module._ai_set(candidate, "default_models", model_dump(config.default_models))

    matching = [key for key in candidate if str(key).casefold() == "default_models"]
    assert len(matching) == 1
    assert config.providers


@pytest.mark.asyncio
async def test_saved_provider_key_cannot_be_sent_to_unsaved_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration as module

    config = module._validate_full(_valid_ai(), strict_references=False)
    monkeypatch.setattr(module, "get_llm_config", lambda: config)

    with pytest.raises(HTTPException) as error:
        await module.discover_models(
            module.ProviderDiscoveryRequest(
                provider_name="Test",
                api_type="openai",
                api_base="https://attacker.invalid/v1",
            )
        )

    assert error.value.detail["code"] == "provider_credentials_scope_mismatch"


@pytest.mark.asyncio
async def test_deepseek_discovery_uses_runtime_default_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration as module

    ai = _valid_ai()
    ai["PROVIDERS"][0].update(api_type="deepseek", api_base=None)
    config = module._validate_full(ai, strict_references=False)
    monkeypatch.setattr(module, "get_llm_config", lambda: config)
    requested_urls: list[str] = []

    class Response:
        status_code = 200

        @staticmethod
        def json() -> dict[str, Any]:
            return {"data": [{"id": "deepseek-chat"}]}

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url: str, **kwargs):
            requested_urls.append(url)
            return Response()

    monkeypatch.setattr(module.httpx, "AsyncClient", lambda **kwargs: Client())

    result = await module.discover_models(
        module.ProviderDiscoveryRequest(provider_name="Test", api_type="deepseek")
    )

    assert result.data["models"] == ["deepseek-chat"]
    assert requested_urls == ["https://api.deepseek.com/v1/models"]


@pytest.mark.asyncio
async def test_model_group_rows_require_unique_non_blank_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import zhenxun.builtin_plugins.web_ui.api.tabs.ai.configuration as module

    config_path = tmp_path / "config.yaml"
    original = _dump_config(config_path, _valid_ai())
    revision = hashlib.sha256(original.encode()).hexdigest()
    monkeypatch.setattr(module, "_CONFIG_FILE", config_path)

    for rows, expected_code in (
        ([{"name": " ", "targets": []}], "model_group_name_required"),
        (
            [
                {"name": "fallback", "targets": []},
                {"name": "fallback", "targets": []},
            ],
            "model_group_name_duplicate",
        ),
    ):
        with pytest.raises(HTTPException) as error:
            await module.update_section(
                "model_groups",
                module.SectionUpdate(expected_revision=revision, value=rows),
            )
        assert error.value.detail["issues"][0]["code"] == expected_code
        assert config_path.read_text(encoding="utf-8") == original
