from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_lifecycle_status_endpoints_return_sanitized_component_data() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.system import (
        get_lifecycle_component,
        get_lifecycle_status,
    )
    from zhenxun.services.lifecycle import ComponentSpec, lifecycle_kernel

    component_id = "test:lifecycle-api"
    if lifecycle_kernel.component_status(component_id) is None:
        lifecycle_kernel.register(
            ComponentSpec(component_id, stage="warmup", source="test"), lambda: None
        )

    status = await get_lifecycle_status()
    component = await get_lifecycle_component(component_id)
    missing = await get_lifecycle_component("test:missing-component")

    assert status.suc is True
    assert status.data is not None
    assert any(
        item["component_id"] == component_id for item in status.data["components"]
    )
    assert component.data is not None
    assert component.data["component_id"] == component_id
    assert "absolute_path" not in component.data
    assert missing.suc is False
    assert missing.code == 404
