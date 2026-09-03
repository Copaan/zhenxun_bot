from pathlib import Path
import subprocess
import sys


def test_webui_tls_import_does_not_initialize_nonebot() -> None:
    root = Path(__file__).resolve().parents[1]
    script = """
import sys

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    pass
else:
    raise AssertionError("NoneBot unexpectedly initialized before TLS import")

from zhenxun.configs.webui_tls import load_webui_tls_settings

assert load_webui_tls_settings().scheme in {"http", "https"}
assert "zhenxun.services" not in sys.modules
assert "nonebot_plugin_apscheduler" not in sys.modules

try:
    nonebot.get_driver()
except ValueError:
    pass
else:
    raise AssertionError("TLS import initialized NoneBot")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_nonebot_store_launcher_import_has_no_nonebot_side_effects() -> None:
    root = Path(__file__).resolve().parents[1]
    script = """
import sys
import nonebot

from zhenxun.nonebot_store.runtime import activate_current_generation

activate_current_generation()
assert "zhenxun.services" not in sys.modules
assert "nonebot_plugin_apscheduler" not in sys.modules
try:
    nonebot.get_driver()
except ValueError:
    pass
else:
    raise AssertionError("NoneBot store launcher import initialized NoneBot")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_source_store_launcher_import_has_no_nonebot_side_effects() -> None:
    root = Path(__file__).resolve().parents[1]
    script = """
import sys
import nonebot

from zhenxun.plugin_store_transaction import pending_transaction

pending_transaction()
assert "zhenxun.services" not in sys.modules
assert "nonebot_plugin_apscheduler" not in sys.modules
try:
    nonebot.get_driver()
except ValueError:
    pass
else:
    raise AssertionError("Source store launcher import initialized NoneBot")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_services_facade_does_not_eagerly_import_heavy_services() -> None:
    root = Path(__file__).resolve().parents[1]
    script = """
import sys
import zhenxun.services

assert "zhenxun.services.ai" not in sys.modules
assert "zhenxun.services.renderer" not in sys.modules
assert "nonebot_plugin_htmlrender" not in sys.modules
assert "nonebot_plugin_apscheduler" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_hook_kernel_install_does_not_preimport_library_plugins() -> None:
    root = Path(__file__).resolve().parents[1]
    script = """
import sys
import nonebot

nonebot.init()
from zhenxun.services.lifecycle import hook_kernel
from zhenxun.services.runtime_reload import plugin_runtime_manager

hook_kernel.install(plugin_runtime_manager)
for name in (
    "nonebot_plugin_apscheduler",
    "nonebot_plugin_alconna",
    "nonebot_plugin_uninfo",
    "nonebot_plugin_waiter",
):
    assert name not in sys.modules, name
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_ai_config_models_do_not_import_alconna_messages() -> None:
    root = Path(__file__).resolve().parents[1]
    script = """
import sys
import nonebot

nonebot.init()
from zhenxun.services.ai.config.models import LLMConfig

assert LLMConfig is not None
assert "zhenxun.services.ai.core.messages" not in sys.modules
assert "nonebot_plugin_alconna" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
