"""媒体转换：silk 编解码、ffmpeg 转码、视频抽帧与图片压缩"""

import asyncio
import io
from pathlib import Path
import re
import shutil
import tempfile
import wave

import pysilk

from zhenxun.services.log import logger

SILK_SAMPLE_RATE = 24000
"""silk 编解码使用的采样率"""
FFMPEG_TIMEOUT = 60
"""单次 ffmpeg 调用的超时秒数"""
_DURATION = re.compile(rb"Duration: (\d+):(\d+):(\d+(?:\.\d+)?)")
IMAGE_TARGET_BYTES = 512 * 1024
"""压缩后图片的目标大小，超过时继续缩小"""
IMAGE_MIN_SIDE = 256
"""继续缩小时长边的下限"""
GIF_FRAMES = 9
"""动图抽取的帧数，拼成 3×3"""


def compress_image(data: bytes, max_side: int) -> tuple[bytes, str, bool] | None:
    """压缩发给模型的图片：长边不超过 max_side，有透明通道的存 PNG，否则存 JPEG
    （质量 85），仍超过 512KB 时继续缩小；动图均匀抽 9 帧拼成 3×3。
    已在限制内的 JPEG / PNG 不处理

    参数:
        data: 图片
        max_side: 长边上限

    返回:
        tuple[bytes, str, bool] | None: (图片, MIME, 是否为动图拼图)，
            无需处理或无法解析时为空
    """
    from PIL import Image, ImageOps

    try:
        img = Image.open(io.BytesIO(data))
        frames = getattr(img, "n_frames", 1)
        if frames > 1:
            picks = sorted(
                {round(i * (frames - 1) / (GIF_FRAMES - 1)) for i in range(GIF_FRAMES)}
            )
            tiles = []
            for index in picks:
                img.seek(index)
                frame = img.convert("RGBA")
                tile = Image.new("RGB", frame.size, "white")
                tile.paste(frame, mask=frame.getchannel("A"))
                tiles.append(tile)
            width, height = tiles[0].size
            img = Image.new("RGB", (width * 3, height * 3), "white")
            for index, tile in enumerate(tiles):
                img.paste(tile, ((index % 3) * width, (index // 3) * height))
            alpha = False
        else:
            if (
                img.format in ("JPEG", "PNG")
                and max(img.size) <= max_side
                and len(data) <= IMAGE_TARGET_BYTES
            ):
                return None
            img = ImageOps.exif_transpose(img)
            alpha = img.mode in ("RGBA", "LA", "PA") or (
                img.mode == "P" and "transparency" in img.info
            )
            img = img.convert("RGBA" if alpha else "RGB")
        side = max_side
        while True:
            out = img.copy()
            out.thumbnail((side, side))
            buffer = io.BytesIO()
            if alpha:
                out.save(buffer, "PNG", optimize=True)
            else:
                out.save(buffer, "JPEG", quality=85, optimize=True)
            if buffer.tell() <= IMAGE_TARGET_BYTES or side <= IMAGE_MIN_SIDE:
                break
            side = max(IMAGE_MIN_SIDE, int(side * 0.75))
    except Exception as e:
        logger.debug(f"图片压缩失败，使用原图: {e}")
        return None
    return buffer.getvalue(), "image/png" if alpha else "image/jpeg", frames > 1


def ffmpeg_path() -> str | None:
    """系统中可用的 ffmpeg 路径

    返回:
        str | None: 可执行文件路径，未安装时为空
    """
    return shutil.which("ffmpeg")


def pcm_to_wav(pcm: bytes, sample_rate: int = SILK_SAMPLE_RATE) -> bytes:
    """把 16 位单声道 PCM 封装为 WAV

    参数:
        pcm: PCM 数据
        sample_rate: 采样率

    返回:
        bytes: WAV 数据
    """
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as writer:
        writer.setnchannels(1)
        writer.setsampwidth(2)
        writer.setframerate(sample_rate)
        writer.writeframes(pcm)
    return buffer.getvalue()


async def _ffmpeg(*args: str, data: bytes | None = None) -> tuple[bytes, bytes] | None:
    """运行 ffmpeg

    参数:
        args: ffmpeg 参数
        data: 写入标准输入的数据

    返回:
        tuple[bytes, bytes] | None: (标准输出, 标准错误)，未安装、超时或失败时为空
    """
    if not (path := ffmpeg_path()):
        return None
    process = await asyncio.create_subprocess_exec(
        path,
        "-hide_banner",
        "-nostdin" if data is None else "-y",
        *args,
        stdin=asyncio.subprocess.PIPE
        if data is not None
        else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(data), FFMPEG_TIMEOUT
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        logger.warning("ffmpeg 处理超时")
        return None
    if process.returncode != 0:
        logger.warning(f"ffmpeg 处理失败: {stderr[-300:].decode(errors='ignore')}")
        return None
    return stdout, stderr


async def silk_to_wav(data: bytes) -> bytes | None:
    """把 silk 语音解码为 WAV

    参数:
        data: silk 数据（可带 QQ 的 0x02 前缀）

    返回:
        bytes | None: WAV 数据，解码失败时为空
    """

    def decode() -> bytes:
        output = io.BytesIO()
        pysilk.decode(io.BytesIO(data), output, SILK_SAMPLE_RATE)
        return pcm_to_wav(output.getvalue())

    try:
        return await asyncio.to_thread(decode)
    except (pysilk.SilkError, ValueError) as e:
        logger.warning(f"silk 语音解码失败: {e}")
        return None


async def voice_to_wav(data: bytes, mime: str) -> bytes | None:
    """把 QQ 语音转为 WAV：silk 本地解码，其他格式（如 amr）需要 ffmpeg

    参数:
        data: 音频数据
        mime: 音频类型

    返回:
        bytes | None: WAV 数据，无法转换时为空
    """
    if mime == "audio/silk":
        return await silk_to_wav(data)
    args = ("-i", "pipe:0", "-f", "wav", "-ac", "1", "-ar", str(SILK_SAMPLE_RATE))
    result = await _ffmpeg(*args, "pipe:1", data=data)
    return result[0] if result and result[0] else None


async def audio_to_silk(data: bytes, audio_format: str) -> bytes | None:
    """把语音转为 QQ 使用的 silk；WAV、PCM 直接编码，其他格式需要 ffmpeg

    参数:
        data: 音频数据
        audio_format: 音频格式，如 wav、pcm、mp3

    返回:
        bytes | None: silk 数据，无法转换时为空
    """
    pcm, sample_rate = None, SILK_SAMPLE_RATE
    if audio_format == "pcm":
        pcm = data
    elif audio_format == "wav":
        try:
            with wave.open(io.BytesIO(data)) as reader:
                if reader.getnchannels() == 1 and reader.getsampwidth() == 2:
                    pcm = reader.readframes(reader.getnframes())
                    sample_rate = reader.getframerate()
        except (wave.Error, EOFError):
            pcm = None
    if pcm is None:
        args = ("-i", "pipe:0", "-f", "s16le", "-ac", "1", "-ar", str(sample_rate))
        result = await _ffmpeg(*args, "pipe:1", data=data)
        if result is None:
            return None
        pcm = result[0]

    def encode() -> bytes:
        output = io.BytesIO()
        pysilk.encode(io.BytesIO(pcm), output, sample_rate, 24000, tencent=True)
        return output.getvalue()

    try:
        return await asyncio.to_thread(encode)
    except (pysilk.SilkError, ValueError) as e:
        logger.warning(f"silk 语音编码失败: {e}")
        return None


async def video_frames(data: bytes, count: int = 4) -> tuple[list[bytes], float | None]:
    """从视频中均匀截取若干帧（JPEG，宽度不超过 640）

    参数:
        data: 视频数据
        count: 截取帧数

    返回:
        tuple[list[bytes], float | None]: (帧图片, 时长秒数)，未安装 ffmpeg
            或无法解析时帧列表为空
    """
    if not ffmpeg_path():
        return [], None
    with tempfile.TemporaryDirectory(prefix="zx-video-") as folder:
        source = Path(folder) / "video"
        source.write_bytes(data)
        probe = await _ffmpeg("-i", str(source), "-f", "null", "-t", "0", "-")
        match = _DURATION.search(probe[1]) if probe else None
        duration = (
            int(match[1]) * 3600 + int(match[2]) * 60 + float(match[3])
            if match
            else None
        )
        if not duration:
            return [], duration
        frames = []
        for index in range(count):
            target = Path(folder) / f"{index}.jpg"
            moment = f"{duration * (index + 0.5) / count:.2f}"
            args = ("-ss", moment, "-i", str(source), "-frames:v", "1")
            if await _ffmpeg(*args, "-vf", "scale='min(640,iw)':-2", str(target)):
                if target.is_file():
                    frames.append(target.read_bytes())
        return frames, duration
