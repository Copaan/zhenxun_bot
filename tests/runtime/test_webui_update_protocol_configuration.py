from __future__ import annotations

from io import StringIO
import json
from pathlib import Path
import subprocess
import sys
import zipfile

from dotenv import dotenv_values
import pytest


def test_update_service_imports_before_nonebot_initialization() -> None:
    project_root = Path(__file__).resolve().parents[2]
    code = (
        "import nonebot\n"
        "try:\n"
        "    nonebot.get_driver()\n"
        "except ValueError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('NoneBot unexpectedly initialized')\n"
        "import zhenxun.update_service\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_protocol_env_update_preserves_multiline_and_masks_credentials() -> None:
    from zhenxun.builtin_plugins.web_ui.api.protocol import configuration

    content = (
        "# keep this comment\n"
        "QQ_ADAPTER_LOAD=False\n"
        'QQ_BOTS=\'\n[\n  {"id":"100","token":"token-value",'
        '"secret":"secret-value"}\n]\n\'\n'
        "HOST=0.0.0.0\n"
    )
    updated = configuration._update_env(
        content,
        {"QQ_ADAPTER_LOAD": True, "QQ_WEBHOOK_PUBLIC_BASE_URL": "https://bot.example"},
    )
    parsed = dotenv_values(stream=StringIO(updated))
    assert parsed["QQ_ADAPTER_LOAD"] == "True"
    assert parsed["QQ_BOTS"].startswith("\n[")
    assert "# keep this comment" in updated

    masked = configuration._masked_configuration(updated)
    serialized = json.dumps(masked)
    assert "token-value" not in serialized
    assert "secret-value" not in serialized
    assert masked["qq"]["bots"] == [
        {"id": "100", "has_token": True, "has_secret": True}
    ]


@pytest.mark.asyncio
async def test_disabling_qq_preserves_existing_bot_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.protocol import configuration

    env = tmp_path / ".env.dev"
    env.write_text(
        "QQ_ADAPTER_LOAD=True\nQQ_BOTS='not-valid-json'\nQQ_WEBHOOK_MODE=external\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(configuration, "_ENV_FILE", env)
    monkeypatch.setattr(configuration, "_ENV_TEMPLATE", tmp_path / ".env.example")
    payload = configuration.ProtocolConfigurationUpdate(
        expected_revision=configuration._revision(env.read_text(encoding="utf-8")),
        qq_enabled=False,
        qq_bots=[],
        qq_webhook_mode="external",
    )
    result = await configuration.save_protocol_configuration(payload)
    assert result.suc
    updated = env.read_text(encoding="utf-8")
    assert "QQ_BOTS='not-valid-json'" in updated
    assert dotenv_values(stream=StringIO(updated))["QQ_ADAPTER_LOAD"] == "False"


@pytest.mark.asyncio
async def test_enabled_qq_reuses_saved_secrets_and_returns_no_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.protocol import configuration

    bots = [
        {
            "id": "100",
            "token": "saved-token",
            "secret": "saved-secret",
            "use_websocket": False,
            "intent": {"c2c_group_at_messages": True},
        }
    ]
    env = tmp_path / ".env.dev"
    env.write_text(
        "QQ_ADAPTER_LOAD=True\n"
        f"QQ_BOTS={json.dumps(bots, separators=(',', ':'))}\n"
        "QQ_WEBHOOK_MODE=external\nHOST=127.0.0.1\nPORT=8080\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(configuration, "_ENV_FILE", env)
    monkeypatch.setattr(configuration, "_ENV_TEMPLATE", tmp_path / ".env.example")

    observed: list[tuple[str, str]] = []

    async def fake_probe(app_id: str, secret: str):
        observed.append((app_id, secret))
        return {"app_id": app_id, "bot_id": "bot", "username": "name"}

    monkeypatch.setattr(configuration, "_probe_credential", fake_probe)
    payload = configuration.ProtocolConfigurationUpdate(
        expected_revision=configuration._revision(env.read_text(encoding="utf-8")),
        qq_enabled=True,
        qq_bots=[configuration.QQBotForm(id="100")],
        qq_webhook_mode="external",
        qq_webhook_public_base_url="https://bot.example.com",
    )
    result = await configuration.save_protocol_configuration(payload)
    assert result.suc
    assert observed == [("100", "saved-secret")]
    serialized = result.model_dump_json()
    assert "saved-token" not in serialized
    assert "saved-secret" not in serialized
    parsed = dotenv_values(stream=StringIO(env.read_text(encoding="utf-8")))
    stored = json.loads(parsed["QQ_BOTS"])
    assert stored[0]["token"] == "saved-token"
    assert stored[0]["secret"] == "saved-secret"


@pytest.mark.asyncio
async def test_qq_probe_reuses_saved_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.protocol import configuration

    env = tmp_path / ".env.dev"
    env.write_text(
        "QQ_BOTS="
        + json.dumps([{"id": "100", "token": "saved-token", "secret": "saved-secret"}])
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(configuration, "_ENV_FILE", env)
    monkeypatch.setattr(configuration, "_ENV_TEMPLATE", tmp_path / ".env.example")
    observed: list[tuple[str, str]] = []

    async def fake_probe(app_id: str, secret: str):
        observed.append((app_id, secret))
        return {"app_id": app_id, "bot_id": "bot", "username": "name"}

    monkeypatch.setattr(configuration, "_probe_credential", fake_probe)
    result = await configuration.probe_qq_credential(
        configuration.QQCredentialProbe(id="100")
    )
    assert result.suc
    assert observed == [("100", "saved-secret")]


def test_update_archive_rejects_path_escape(tmp_path: Path) -> None:
    from zhenxun import update_service

    archive = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("../outside.txt", "bad")
    with pytest.raises(update_service.UpdateServiceError, match="archive_path_escape"):
        update_service._safe_extract(archive, tmp_path / "extract")
    assert not (tmp_path / "outside.txt").exists()


def test_webui_apply_preserves_nested_git_and_rolls_visible_assets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun import update_service

    public = tmp_path / "data" / "web_ui" / "public"
    (public / ".git").mkdir(parents=True)
    (public / ".git" / "config").write_text("preserve", encoding="utf-8")
    (public / "old.js").write_text("old", encoding="utf-8")
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "index.html").write_text("new", encoding="utf-8")
    (staged / "version.json").write_text('{"version":"0.1.0"}', encoding="utf-8")

    monkeypatch.setattr(update_service, "_ROOT", tmp_path)
    monkeypatch.setattr(update_service, "_BACKUP_ROOT", tmp_path / "backups")
    update_service._apply_webui("a" * 32, staged)

    assert (public / ".git" / "config").read_text(encoding="utf-8") == "preserve"
    assert (public / "index.html").read_text(encoding="utf-8") == "new"
    assert not (public / "old.js").exists()


def test_webui_staging_requires_compatible_manifest_and_assets(
    tmp_path: Path,
) -> None:
    from zhenxun import update_service

    (tmp_path / "js").mkdir()
    (tmp_path / "css").mkdir()
    (tmp_path / "js" / "app.hash.js").write_text("", encoding="utf-8")
    (tmp_path / "css" / "app.hash.css").write_text("", encoding="utf-8")
    (tmp_path / "favicon.ico").write_bytes(b"ico")
    (tmp_path / "index.html").write_text(
        '<script src="/js/app.hash.js"></script>'
        '<link href="/css/app.hash.css" rel="stylesheet">'
        '<link href="/favicon.ico" rel="icon">',
        encoding="utf-8",
    )
    (tmp_path / "version.json").write_text(
        '{"version":"0.1.0","api_version":1}', encoding="utf-8"
    )
    update_service._validate_staging("webui", tmp_path)

    (tmp_path / "version.json").write_text(
        '{"version":"0.1.0","api_version":2}', encoding="utf-8"
    )
    with pytest.raises(
        update_service.UpdateServiceError, match="webui_api_incompatible"
    ):
        update_service._validate_staging("webui", tmp_path)


def test_webui_status_compares_matching_source_commits() -> None:
    from zhenxun import update_service

    status = update_service._component_status(
        "webui",
        "0.1.0",
        "source-commit",
        {
            "version": "0.1.0",
            "commit": "source-commit",
            "manifest": True,
            "api_version": 1,
        },
    )
    assert status["update_available"] is False
    assert status["compatible"] is True


def test_blocked_bot_release_is_visible_but_not_updateable() -> None:
    from zhenxun import update_service

    status = update_service._component_status(
        "bot",
        "0.2.3",
        None,
        {"version": "v0.2.4-fix", "ref": "v0.2.4-fix", "manifest": True},
        "release",
    )
    assert status["blocked"] is True
    assert status["block_reason"]
    assert status["update_available"] is False


@pytest.mark.asyncio
async def test_blocked_bot_release_cannot_bypass_job_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun import update_service

    async def blocked_ref(*_args):
        return "0.2.4-fix"

    monkeypatch.setattr(update_service, "_latest_ref", blocked_ref)
    monkeypatch.setattr(update_service, "_JOBS_ROOT", tmp_path / "jobs")
    monkeypatch.setattr(update_service, "_PENDING_FILE", tmp_path / "pending.json")
    monkeypatch.setattr(update_service, "_ACTIVE_TASK", None)
    with pytest.raises(update_service.UpdateServiceError, match="release_blocked"):
        await update_service.create_update_job(
            component="bot",
            channel="release",
            method="git",
            source="aliyun",
            force=False,
        )


def test_bot_update_can_roll_back_after_health_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun import update_service

    staging = tmp_path / "data" / "update" / "staging"
    backups = tmp_path / "data" / "update" / "backups"
    jobs = tmp_path / "data" / "update" / "jobs"
    pending = tmp_path / "data" / "update" / "pending.json"
    applied = tmp_path / "data" / "update" / "applied.json"
    for name, value in {
        "_ROOT": tmp_path,
        "_STAGING_ROOT": staging,
        "_BACKUP_ROOT": backups,
        "_JOBS_ROOT": jobs,
        "_PENDING_FILE": pending,
        "_APPLIED_FILE": applied,
    }.items():
        monkeypatch.setattr(update_service, name, value)

    (tmp_path / "zhenxun").mkdir()
    (tmp_path / "zhenxun" / "state.txt").write_text("old", encoding="utf-8")
    staged = staging / ("a" * 32) / "source"
    (staged / "zhenxun").mkdir(parents=True)
    (staged / "zhenxun" / "state.txt").write_text("new", encoding="utf-8")
    (staged / "__version__").write_text("new", encoding="utf-8")
    job_id = "a" * 32
    update_service._write_json(
        jobs / f"{job_id}.json",
        {"job_id": job_id, "component": "bot", "state": "pending_restart"},
    )
    update_service._write_json(
        pending,
        {
            "job_id": job_id,
            "component": "bot",
            "staged_root": str(staged.relative_to(tmp_path)),
        },
    )

    assert update_service.apply_pending_update(tmp_path)
    assert (tmp_path / "zhenxun" / "state.txt").read_text(encoding="utf-8") == "new"
    assert applied.is_file()
    assert update_service.rollback_applied_update()
    assert (tmp_path / "zhenxun" / "state.txt").read_text(encoding="utf-8") == "old"
    assert not (tmp_path / "__version__").exists()
    assert update_service.read_job(job_id)["error"] == "health_check_failed_rolled_back"
