from pathlib import Path

import nonebot

path = Path(__file__).parent

nonebot.load_plugins(str((path / "qq").resolve()))
