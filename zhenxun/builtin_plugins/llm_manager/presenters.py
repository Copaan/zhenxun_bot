import time
from typing import Any, Literal

from zhenxun import ui
from zhenxun.services import renderer_service
from zhenxun.services.ai.core.models import ModelModality
from zhenxun.services.ai.llm.client import Route
from zhenxun.ui.models import StatusBadgeCell, TextCell


def _format_seconds(seconds: int) -> str:
    """将秒数格式化为 'Xm Ys' 或 'Xh Ym' 的形式"""
    if seconds <= 0:
        return "0s"
    if seconds < 60:
        return f"{seconds}s"

    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


class Presenters:
    """格式化LLM管理插件的输出 (图片格式)"""

    @staticmethod
    async def format_model_list_as_image(
        models: list[dict[str, Any]], show_all: bool
    ) -> bytes:
        """将模型列表格式化为表格图片"""
        title = "LLM模型列表" + (" (所有已配置模型)" if show_all else " (仅可用)")

        if not models:
            table = ui.table(title=title, tip="当前没有配置任何LLM模型。").set_headers(
                ["提供商", "模型名称", "协议", "状态"]
            )
            return await renderer_service.render(table)

        column_name = ["提供商", "模型名称", "协议", "状态"]
        rows_data = []
        for model in models:
            is_available = model.get("is_available", True)
            embed_tag = " (Embed)" if model.get("is_embedding_model", False) else ""
            rows_data.append(
                [
                    TextCell(content=model.get("provider_name", "N/A")),
                    TextCell(content=f"{model.get('model_name', 'N/A')}{embed_tag}"),
                    TextCell(content=model.get("protocol") or "N/A"),
                    StatusBadgeCell(
                        text="可用" if is_available else "不可用",
                        status_type="ok" if is_available else "error",
                    ),
                ]
            )

        table = ui.table(
            title=title, tip="使用 `llm info <Provider/ModelName>` 查看详情"
        )
        table.set_headers(column_name)
        table.set_column_alignments(["left", "left", "left", "center"])
        table.add_rows(rows_data)
        return await renderer_service.render(table, use_cache=True)

    @staticmethod
    async def format_model_details_as_markdown_image(details: dict[str, Any]) -> bytes:
        """将模型详情格式化为Markdown图片"""
        provider = details["provider"]
        model = details["model"]
        profile = details["profile"]
        route = Route(provider, model, profile)

        modality_names = {
            ModelModality.IMAGE: "图片",
            ModelModality.VIDEO: "视频",
            ModelModality.AUDIO: "音频",
        }
        cap_list = [
            name
            for modality, name in modality_names.items()
            if modality in profile.input_modalities
        ]
        if profile.tools:
            cap_list.append("工具调用")
        if profile.reasoning_levels:
            cap_list.append(f"推理 ({'/'.join(profile.reasoning_levels)})")

        md = ui.markdown("")
        md.head(f"🔎 模型详情: {route.name}", 1)
        md.text("---")
        md.head("提供商信息", 2)
        md.text(f"- **名称**: {provider.name}")
        md.text(f"- **预设**: {provider.preset or '自定义'}")
        md.text(f"- **协议**: {route.protocol}")
        md.text(f"- **Base URL**: {route.base_url}")

        md.head("模型详情", 2)

        params = route.generation_config(None).common
        temp_value = params.temperature if params.temperature is not None else "未设置"
        tokens = profile.context_window
        context_window = f"{tokens // 1000}K" if tokens >= 1000 else str(tokens)

        md.text(f"- **名称**: {model.name}")
        md.text(f"- **状态**: {'启用' if model.enabled else '禁用'}")
        md.text(f"- **任务**: {', '.join(sorted(profile.tasks))}")
        md.text(f"- **默认温度**: {temp_value}")
        md.text(f"- **上下文窗口**: {context_window}")
        md.text(f"- **核心能力**: {', '.join(cap_list) or '纯文本'}")

        return await renderer_service.render(md.with_style("light"))

    @staticmethod
    async def format_key_status_as_image(
        provider_name: str, sorted_stats: list[dict[str, Any]]
    ) -> bytes:
        """将已排序的、详细的API Key状态格式化为表格图片"""
        title = f"🔑 '{provider_name}' API Key 状态"

        data_list = []

        for key_info in sorted_stats:
            status_str = key_info.get("status", "healthy")
            successes = key_info.get("successes", 0)
            failures = key_info.get("failures", 0)
            total_calls = successes + failures

            if total_calls == 0 and status_str == "healthy":
                status_str = "unused"

            if status_str == "cooldown":
                cooldown_seconds = max(
                    0, int(key_info.get("cooldown_until", 0) - time.time())
                )
                formatted_time = _format_seconds(cooldown_seconds)
                status_cell = StatusBadgeCell(
                    text=f"冷却中({formatted_time})", status_type="info"
                )
            else:
                status_map: dict[
                    str,
                    tuple[str, Literal["ok", "error", "warning", "info", "success"]],
                ] = {
                    "invalid": ("永久禁用", "error"),
                    "healthy": ("健康", "ok"),
                    "unused": ("未使用", "info"),
                }
                text, status_type = status_map.get(status_str, ("未知", "info"))
                status_cell = StatusBadgeCell(text=text, status_type=status_type)

            total_calls_text = (
                f"{successes}/{total_calls}" if total_calls > 0 else "0/0"
            )

            success_rate = (successes / total_calls * 100) if total_calls > 0 else 100.0
            success_rate_text = f"{success_rate:.1f}%" if total_calls > 0 else "N/A"
            rate_color = None
            if total_calls > 0:
                if success_rate < 80:
                    rate_color = "#F56C6C"
                elif success_rate < 95:
                    rate_color = "#E6A23C"
            success_rate_cell = TextCell(content=success_rate_text, color=rate_color)

            last_error = key_info.get("last_error") or "-"
            if len(last_error) > 25:
                last_error = last_error[:22] + "..."

            suggested_action = "-"
            if status_str == "invalid":
                suggested_action = "检查配额或换Key"
            elif status_str == "cooldown":
                suggested_action = "等待恢复"

            data_list.append(
                [
                    TextCell(content=key_info["key_id"]),
                    status_cell,
                    TextCell(content=total_calls_text),
                    success_rate_cell,
                    TextCell(content=last_error),
                    TextCell(content=suggested_action),
                ]
            )

        table = ui.table(title=title, tip="使用 `llm reset-key <Provider>` 重置Key状态")
        table.set_headers(
            [
                "Key (部分)",
                "状态",
                "总调用",
                "成功率",
                "上次错误",
                "建议操作",
            ]
        )
        table.add_rows(data_list)
        return await renderer_service.render(table, use_cache=False)

    @staticmethod
    async def format_mcp_list_as_image(mcp_list: list[dict[str, Any]]) -> bytes:
        """将MCP列表格式化为表格图片"""
        title = "MCP 服务管理列表"
        if not mcp_list:
            table = ui.table(title=title, tip="当前未配置任何 MCP 服务。").set_headers(
                ["ID", "MCP名称", "协议", "状态", "目标"]
            )
            return await renderer_service.render(table)

        column_name = ["ID", "MCP名称", "协议", "状态", "目标"]
        rows_data = []
        for mcp in mcp_list:
            is_enable = mcp["enabled"]
            status_type = "success" if is_enable else "info"
            status_text = "开启" if is_enable else "关闭"
            rows_data.append(
                [
                    TextCell(content=str(mcp["id"])),
                    TextCell(content=mcp["name"]),
                    TextCell(content=mcp["transport"]),
                    StatusBadgeCell(text=status_text, status_type=status_type),
                    TextCell(content=mcp["target"]),
                ]
            )

        table = ui.table(
            title=title,
            tip="使用 `llm mcp 开启/关闭 <ID/名称>` 来修改状态，支持批量操作",
        )
        table.set_headers(column_name)
        table.set_column_alignments(["center", "left", "left", "center", "left"])
        table.add_rows(rows_data)
        return await renderer_service.render(table, use_cache=False)
