"""角色识别的本地模型：下载、角色库生成与推理

模型与角色库从 HF 镜像下载，保存在 data/ai/characters。推理依赖可选组件
characters（onnxruntime），在线程中运行，同一时间只处理一张图。
"""

import asyncio
import csv
from dataclasses import dataclass
import io
import json
from pathlib import Path
import re
import shutil
import threading
import time
from typing import Any, ClassVar

import httpx
import numpy as np
from PIL import Image

from zhenxun.configs.config import Config
from zhenxun.configs.path_config import DATA_PATH
from zhenxun.services.log import logger
from zhenxun.utils.http_utils import get_async_client
from zhenxun.utils.user_agent import get_user_agent

from ._components import installed
from ._data_source import MODULE

CHAR_DIR = DATA_PATH / "ai" / "characters"
"""本地模型与角色库目录"""
TAGGERS = {
    "full": ("SmilingWolf/wd-eva02-large-tagger-v3", False),
    "lite": ("SmilingWolf/wd-swinv2-tagger-v3", True),
}
"""档位对应的打标模型仓库，以及是否在安装时量化为 int8"""
CCIP = ("deepghs/ccip_onnx", "ccip-caformer-24-randaug-pruned/model_feat.onnx")
"""CCIP 角色特征模型"""
INDEX_REPO = "datasets/deepghs/character_index"
"""角色库特征来源"""
MIN_POSTS, NEW_ID, NEW_POSTS = 1000, 1900000, 300
"""收录帖子数 ≥1000 的角色，或标签编号 ≥1900000（2022 年后）且帖子数 ≥300 的新角色"""
PROTOTYPES = 8
"""每个角色保留的原型数"""
GENERAL_THRESHOLD = 0.35
"""外观标签的置信度阈值"""
TAGGER_THRESHOLD = 0.5
"""角色标签的置信度阈值"""
LIBRARY_THRESHOLD, LIBRARY_MARGIN = 0.8, 0.02
"""角色库比对的余弦阈值，以及第一名需领先第二名的差距"""
COMPANION_COVER, COMPANION_GENDER = 0.8, 0.2
"""伴生合并：核心标签覆盖率与女性比例差的阈值"""
COMPANION_SIMILARITY, COMPANION_MINOR, COMPANION_RATIO = 0.98, 0.96, 0.3
"""伴生合并的特征中心余弦阈值；帖子数不到主角色 30% 的条目放宽到 0.96"""
IDLE_SECONDS = 600
"""模型空闲多久后释放"""


@dataclass
class Candidate:
    key: str
    """合并用标识：danbooru 角色标签，或“名字（作品）”"""
    source: str
    """来源标识"""
    score: float
    """归一化来源分数：通过来源自身阈值时为 0.625，满分 1"""
    raw: float
    """来源的原始分数"""
    work: str | None = None
    """作品"""


def normalize(value: float, threshold: float, top: float) -> float:
    """把来源原始分数映射为来源分数，阈值处为 0.625，满分为 1

    参数:
        value: 原始分数
        threshold: 来源阈值
        top: 原始分数满分

    返回:
        float: 来源分数
    """
    ratio = (value - threshold) / (top - threshold) if top > threshold else 1
    return round(0.625 + 0.375 * min(max(ratio, 0), 1), 4)


def _rgb(image: Image.Image) -> Image.Image:
    """取第一帧，透明背景填白后转为 RGB"""
    image.seek(0)
    if image.mode in ("RGBA", "LA", "P"):
        image = image.convert("RGBA")
        background = Image.new("RGBA", image.size, (255, 255, 255, 255))
        background.alpha_composite(image)
        image = background
    return image.convert("RGB")


def _tagger_input(image: Image.Image, size: int) -> np.ndarray:
    """打标模型输入：补成白底正方形，缩放后转为 BGR"""
    w, h = image.size
    side = max(w, h)
    square = Image.new("RGB", (side, side), (255, 255, 255))
    square.paste(image, ((side - w) // 2, (side - h) // 2))
    array = np.asarray(
        square.resize((size, size), Image.Resampling.BICUBIC), np.float32
    )
    return array[:, :, ::-1][None].copy()


def _ccip_input(image: Image.Image) -> np.ndarray:
    """CCIP 输入：缩放到 384 并按 CLIP 均值方差归一化"""
    array = (
        np.asarray(image.resize((384, 384), Image.Resampling.BILINEAR), np.float32)
        / 255
    )
    mean = np.array([0.48145466, 0.4578275, 0.40821073], np.float32)
    std = np.array([0.26862954, 0.26130258, 0.27577711], np.float32)
    return ((array - mean) / std).transpose(2, 0, 1)[None].astype(np.float32)


def merge_map(characters: list[dict[str, Any]]) -> dict[str, str]:
    """计算服装变体到本体的映射：去掉括号段后的标签存在时归到该标签

    参数:
        characters: character_index 的角色列表

    返回:
        dict[str, str]: 标签 -> 本体标签
    """
    tags = {c["tag"] for c in characters}

    def variant(tag: str) -> str:
        groups = list(re.finditer(r"_\([^()]*\)", tag))
        for g in groups[:-1] if len(groups) > 1 else groups:
            base = tag[: g.start()] + tag[g.end() :]
            if base in tags:
                return variant(base)
        return tag

    return {t: variant(t) for t in tags}


def companion_map(
    characters: list[dict[str, Any]],
    variants: dict[str, str],
    centroids: dict[str, np.ndarray],
) -> dict[str, str]:
    """计算伴生条目到主角色的映射

    同一作品中核心标签 ≥80% 被更热门角色覆盖、特征中心几乎相同且性别比例相近的
    条目（吉祥物、变身形态、异名等）归到该角色，只用于合并角色库原型。
    帖子数接近的两个角色只在特征中心余弦 ≥0.98 时合并，避免把常同框的角色混为一谈。

    参数:
        characters: character_index 的角色列表
        variants: merge_map 的结果
        centroids: 标签 -> 归一化的特征中心，只含已下载特征的标签

    返回:
        dict[str, str]: 本体标签 -> 主角色本体标签，不合并的不在其中
    """
    by_tag = {c["tag"]: c for c in characters}
    grouped: dict[str, list[np.ndarray]] = {}
    for tag, center in centroids.items():
        grouped.setdefault(variants[tag], []).append(center)
    center = {}
    for base, items in grouped.items():
        mean = np.mean(items, axis=0)
        center[base] = mean / np.linalg.norm(mean)
    by_work: dict[str, list[str]] = {}
    for base in center:
        by_work.setdefault(by_tag[base]["copyright"], []).append(base)

    def girl(c: dict[str, Any]) -> float:
        return (c.get("gender") or {}).get("girl", 0.5)

    owner = {}
    for base in center:
        x = by_tag[base]
        core = set(x["core_tags"])
        best = None
        for other in by_work[x["copyright"]] if core else []:
            y = by_tag[other]
            minor = x["post_count"] <= COMPANION_RATIO * y["post_count"]
            similarity = COMPANION_MINOR if minor else COMPANION_SIMILARITY
            if (
                y["post_count"] > x["post_count"]
                and len(core & set(y["core_tags"])) / len(core) >= COMPANION_COVER
                and abs(girl(x) - girl(y)) <= COMPANION_GENDER
                and float(center[base] @ center[other]) >= similarity
                and (best is None or y["post_count"] > best["post_count"])
            ):
                best = y
        if best:
            owner[base] = best["tag"]
    for base in owner:
        while owner[base] in owner:
            owner[base] = owner[owner[base]]
    return owner


def centroid(features: np.ndarray) -> np.ndarray:
    """特征的归一化中心

    参数:
        features: 特征矩阵

    返回:
        np.ndarray: 归一化的中心
    """
    x = features.astype(np.float32)
    x /= np.linalg.norm(x, axis=1, keepdims=True)
    mean = x.mean(0)
    return mean / np.linalg.norm(mean)


def prototypes(features: np.ndarray, seed: int) -> np.ndarray:
    """用余弦 k-means 把一个角色的特征压缩为原型

    参数:
        features: 特征矩阵
        seed: 随机种子

    返回:
        np.ndarray: 归一化的原型，float16
    """
    x = features.astype(np.float32)
    x /= np.linalg.norm(x, axis=1, keepdims=True)
    k = min(PROTOTYPES, len(x))
    c = x[np.random.default_rng(seed).choice(len(x), k, replace=False)]
    for _ in range(10):
        label = (x @ c.T).argmax(1)
        c = np.stack(
            [x[label == q].mean(0) if (label == q).any() else c[q] for q in range(k)]
        )
        c /= np.linalg.norm(c, axis=1, keepdims=True)
    return c.astype(np.float16)


class Library:
    """角色库：按合并后的角色保存原型、作品、核心外观标签与性别"""

    def __init__(self, path: Path):
        data = np.load(path / "index.npz")
        meta = json.loads((path / "index.json").read_text("utf-8"))
        self.vectors = data["vectors"].astype(np.float32)
        self.owner = data["owner"]
        self.names: list[str] = meta["names"]
        self.works: list[str] = meta["works"]
        self.core = [set(c) for c in meta["core"]]
        self.gender: list[dict[str, float]] = meta["gender"]
        self.alias: dict[str, str] = meta["alias"]
        self.position = {n: i for i, n in enumerate(self.names)}

    def work(self, tag: str) -> str | None:
        """角色所属作品，不在库中时为 None"""
        index = self.position.get(self.alias.get(tag, tag))
        return self.works[index] if index is not None else None

    def consistent(self, index: int, general: set[str]) -> bool:
        """候选的核心外观标签至少一半出现在打标结果中，且性别不冲突"""
        core = self.core[index]
        if core and len(core & general) / len(core) < 0.5:
            return False
        gender = self.gender[index]
        if "1boy" in general and "1girl" not in general:
            return gender.get("girl", 0) <= 0.8
        if "1girl" in general and "1boy" not in general:
            return gender.get("boy", 0) <= 0.8
        return True

    def match(self, feature: np.ndarray, general: set[str]) -> list[Candidate]:
        """比对 CCIP 特征，返回外观一致、余弦达到阈值的前 3 个角色

        第一名领先第二名不足 LIBRARY_MARGIN 时，与其余候选一样只计一半分数。

        参数:
            feature: 归一化的 CCIP 特征
            general: 打标模型给出的外观标签

        返回:
            list[Candidate]: 候选
        """
        best = np.full(len(self.names), -1.0, np.float32)
        np.maximum.at(best, self.owner, self.vectors @ feature)
        order = [
            int(i) for i in np.argsort(-best)[:20] if self.consistent(int(i), general)
        ]
        result = []
        for rank, i in enumerate(order[:3]):
            if best[i] < LIBRARY_THRESHOLD:
                break
            score = normalize(float(best[i]), LIBRARY_THRESHOLD, 1.0)
            lead = len(order) < 2 or best[i] - best[order[1]] >= LIBRARY_MARGIN
            result.append(
                Candidate(
                    self.names[i],
                    "library",
                    score if rank == 0 and lead else round(score / 2, 4),
                    round(float(best[i]), 4),
                    self.works[i],
                )
            )
        return result


class LocalModels:
    """本地模型的安装、加载与推理"""

    task: ClassVar[dict[str, Any] | None] = None
    """安装任务进度"""
    _job: ClassVar[asyncio.Task | None] = None
    _sessions: ClassVar[dict[str, Any]] = {}
    _tags: ClassVar[dict[str, tuple[np.ndarray, np.ndarray]]] = {}
    _library: ClassVar[Library | None] = None
    _lock = threading.Lock()
    _release: ClassVar[asyncio.TimerHandle | None] = None

    @staticmethod
    def tagger_file(tier: str) -> Path:
        return CHAR_DIR / f"tagger_{tier}.onnx"

    @staticmethod
    def tags_file(tier: str) -> Path:
        return CHAR_DIR / f"tags_{tier}.json"

    @classmethod
    def ready(cls, tier: str) -> bool:
        """档位的模型与角色库是否都已就绪"""
        return tier in TAGGERS and all(
            p.exists()
            for p in (
                cls.tagger_file(tier),
                cls.tags_file(tier),
                CHAR_DIR / "ccip.onnx",
                CHAR_DIR / "index.npz",
                CHAR_DIR / "index.json",
            )
        )

    @classmethod
    def status(cls) -> dict[str, Any]:
        """各档位与角色库的安装状态

        返回:
            dict[str, Any]: 组件与各档位是否就绪、磁盘占用、角色库信息与任务进度
        """
        files = list(CHAR_DIR.glob("*")) if CHAR_DIR.exists() else []
        library = None
        if (meta := CHAR_DIR / "index.json").exists():
            info = json.loads(meta.read_text("utf-8"))
            library = {"characters": len(info["names"]), "built_at": info["built_at"]}
        return {
            "component": installed("characters"),
            "tiers": {tier: cls.ready(tier) for tier in TAGGERS},
            "disk_mb": round(sum(f.stat().st_size for f in files if f.is_file()) / 1e6),
            "library": library,
            "task": cls.task,
        }

    @staticmethod
    def _url(repo: str, file: str) -> str:
        endpoint = Config.get_config(MODULE, "HF_ENDPOINT") or "https://hf-mirror.com"
        return f"{str(endpoint).rstrip('/')}/{repo}/resolve/main/{file}"

    @classmethod
    async def _download(cls, client: httpx.AsyncClient, url: str, path: Path) -> None:
        """流式下载到临时文件，完成后替换，并累计下载进度"""
        part = path.with_name(path.name + ".part")
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            if cls.task is not None:
                cls.task["total"] += int(response.headers.get("content-length") or 0)
            with part.open("wb") as f:
                async for chunk in response.aiter_bytes(1 << 20):
                    f.write(chunk)
                    if cls.task is not None:
                        cls.task["done"] += len(chunk)
        part.replace(path)

    @classmethod
    def _stage(cls, stage: str, total: int = 0) -> None:
        if cls.task is not None:
            cls.task.update(stage=stage, done=0, total=total)

    @classmethod
    def install(cls, tier: str, rebuild_library: bool = False) -> bool:
        """在后台下载档位所需的模型并生成角色库

        参数:
            tier: lite / full
            rebuild_library: 是否重新生成已有的角色库

        返回:
            bool: 是否已开始，已有任务在运行时为 False
        """
        if tier not in TAGGERS:
            raise ValueError("只有 lite / full 档需要安装本地模型。")
        if not installed("characters"):
            raise ValueError("请先安装“角色识别本地模型”组件，重启后再下载模型。")
        if cls._job and not cls._job.done():
            return False
        cls.task = {"tier": tier, "stage": "准备", "done": 0, "total": 0, "error": None}
        cls._job = asyncio.create_task(cls._install(tier, rebuild_library))
        return True

    @classmethod
    async def _install(cls, tier: str, rebuild_library: bool) -> None:
        repo, quantize = TAGGERS[tier]
        CHAR_DIR.mkdir(parents=True, exist_ok=True)
        try:
            async with get_async_client(
                headers=get_user_agent(),
                follow_redirects=True,
                timeout=httpx.Timeout(60, connect=15),
            ) as client:
                if not cls.tagger_file(tier).exists():
                    cls._stage("下载打标模型")
                    source = CHAR_DIR / f"tagger_{tier}.fp32.onnx"
                    await cls._download(client, cls._url(repo, "model.onnx"), source)
                    if quantize:
                        cls._stage("量化打标模型")
                        await asyncio.to_thread(
                            cls._quantize, source, cls.tagger_file(tier)
                        )
                        source.unlink()
                    else:
                        source.replace(cls.tagger_file(tier))
                if not cls.tags_file(tier).exists():
                    response = await client.get(cls._url(repo, "selected_tags.csv"))
                    response.raise_for_status()
                    rows = list(csv.DictReader(io.StringIO(response.text)))
                    cls.tags_file(tier).write_text(
                        json.dumps(
                            {
                                "names": [r["name"] for r in rows],
                                "categories": [int(r["category"]) for r in rows],
                            }
                        ),
                        "utf-8",
                    )
                if not (CHAR_DIR / "ccip.onnx").exists():
                    cls._stage("下载角色特征模型")
                    await cls._download(client, cls._url(*CCIP), CHAR_DIR / "ccip.onnx")
                if rebuild_library or not (CHAR_DIR / "index.npz").exists():
                    await cls._build_library(client)
            await asyncio.to_thread(cls.unload)
            cls._stage("完成")
            logger.info(f"角色识别 {tier} 档模型已就绪", "AI角色识别")
        except Exception as e:
            logger.warning(f"角色识别模型安装失败: {e}", "AI角色识别", e=e)
            if cls.task is not None:
                cls.task["error"] = str(e) or type(e).__name__
        finally:
            for part in CHAR_DIR.glob("*.part"):
                part.unlink(missing_ok=True)

    @staticmethod
    def _quantize(source: Path, target: Path) -> None:
        from onnxruntime.quantization import QuantType, quantize_dynamic

        quantize_dynamic(source, target, weight_type=QuantType.QUInt8)

    @classmethod
    async def _build_library(cls, client: httpx.AsyncClient) -> None:
        """下载入选角色的特征，压缩为原型后生成角色库"""
        cls._stage("下载角色列表")
        response = await client.get(cls._url(INDEX_REPO, "characters.json"))
        response.raise_for_status()
        characters: list[dict[str, Any]] = response.json()
        variants = merge_map(characters)
        picked = [
            c
            for c in characters
            if c["post_count"] >= MIN_POSTS
            or (c["id"] >= NEW_ID and c["post_count"] >= NEW_POSTS)
        ]
        cls._stage("下载角色特征", len(picked))
        semaphore = asyncio.Semaphore(16)
        protos: dict[str, np.ndarray] = {}
        centroids: dict[str, np.ndarray] = {}
        failed = []

        async def fetch(index: int, c: dict[str, Any]) -> None:
            url = cls._url(INDEX_REPO, f"{c['hprefix']}/{c['short_tag']}/feat.npy")
            async with semaphore:
                for attempt in range(3):
                    try:
                        r = await client.get(url)
                        r.raise_for_status()
                        features = np.load(io.BytesIO(r.content), allow_pickle=False)
                        protos[c["tag"]] = await asyncio.to_thread(
                            prototypes, features, index
                        )
                        centroids[c["tag"]] = centroid(features)
                        break
                    except Exception as e:
                        if attempt == 2:
                            failed.append(c["tag"])
                            logger.debug(f"角色特征 {c['tag']} 下载失败: {e}")
                        await asyncio.sleep(1)
            if cls.task is not None:
                cls.task["done"] += 1

        await asyncio.gather(*(fetch(i, c) for i, c in enumerate(picked)))
        if len(failed) > len(picked) // 20:
            raise RuntimeError(f"{len(failed)} 个角色的特征下载失败，请稍后重试")
        cls._stage("生成角色库")
        companions = companion_map(characters, variants, centroids)
        await asyncio.to_thread(
            cls._save_library, characters, variants, companions, protos
        )

    @staticmethod
    def _save_library(
        characters: list[dict[str, Any]],
        variants: dict[str, str],
        companions: dict[str, str],
        protos: dict[str, np.ndarray],
    ) -> None:
        """保存角色库：原型归到合并后的角色，核心标签只取本体与服装变体"""
        by_tag = {c["tag"]: c for c in characters}
        merged = {t: companions.get(variants[t], variants[t]) for t in protos}
        names = sorted(set(merged.values()))
        position = {n: i for i, n in enumerate(names)}
        vectors = np.concatenate([protos[t] for t in protos])
        owner = np.concatenate(
            [np.full(len(protos[t]), position[merged[t]], np.int32) for t in protos]
        )
        core: dict[str, set[str]] = {n: set() for n in names}
        for c in characters:
            if (base := variants[c["tag"]]) in core:
                core[base].update(c["core_tags"])
        meta = {
            "built_at": int(time.time()),
            "names": names,
            "works": [by_tag[n]["copyright"] for n in names],
            "core": [sorted(core[n]) for n in names],
            "gender": [by_tag[n]["gender"] or {} for n in names],
            "alias": {t: b for t, b in variants.items() if t != b and b in position},
        }
        np.savez(CHAR_DIR / "index.tmp.npz", vectors=vectors, owner=owner)
        (CHAR_DIR / "index.tmp.json").write_text(json.dumps(meta), "utf-8")
        (CHAR_DIR / "index.tmp.npz").replace(CHAR_DIR / "index.npz")
        (CHAR_DIR / "index.tmp.json").replace(CHAR_DIR / "index.json")

    @classmethod
    async def remove(cls) -> None:
        """删除所有本地模型与角色库"""
        if cls._job and not cls._job.done():
            raise ValueError("模型正在下载，请等待完成后再卸载。")
        await asyncio.to_thread(cls.unload)
        await asyncio.to_thread(shutil.rmtree, CHAR_DIR, True)
        cls.task = None

    @classmethod
    def unload(cls, blocking: bool = True) -> bool:
        """释放已加载的模型与角色库

        参数:
            blocking: 是否等待正在进行的推理结束

        返回:
            bool: 是否已释放，不等待且正在推理时为 False
        """
        if not cls._lock.acquire(blocking=blocking):
            return False
        try:
            cls._sessions.clear()
            cls._tags.clear()
            cls._library = None
        finally:
            cls._lock.release()
        return True

    @classmethod
    def _idle(cls) -> None:
        """空闲到期时释放模型，正在推理则顺延"""
        if not cls.unload(blocking=False):
            cls._release = asyncio.get_running_loop().call_later(
                IDLE_SECONDS, cls._idle
            )

    @classmethod
    def library(cls) -> Library | None:
        """已加载的角色库，用于把云端结果的标签归到合并后的角色"""
        return cls._library

    @classmethod
    def _session(cls, name: str, path: Path) -> Any:
        if name not in cls._sessions:
            import onnxruntime as ort

            options = ort.SessionOptions()
            options.intra_op_num_threads = 4
            cls._sessions[name] = ort.InferenceSession(
                str(path), options, providers=["CPUExecutionProvider"]
            )
        return cls._sessions[name]

    @classmethod
    def _run(cls, data: bytes, tier: str) -> dict[str, list[Candidate]]:
        with cls._lock:
            image = _rgb(Image.open(io.BytesIO(data)))
            tagger = cls._session(tier, cls.tagger_file(tier))
            if tier not in cls._tags:
                info = json.loads(cls.tags_file(tier).read_text("utf-8"))
                cls._tags[tier] = (
                    np.array(info["names"]),
                    np.array(info["categories"]),
                )
            if cls._library is None:
                cls._library = Library(CHAR_DIR)
            names, categories = cls._tags[tier]
            library = cls._library
            size = tagger.get_inputs()[0].shape[1]
            probs = tagger.run(
                None, {tagger.get_inputs()[0].name: _tagger_input(image, size)}
            )[0][0]
            general = {
                str(t) for t in names[(categories == 0) & (probs >= GENERAL_THRESHOLD)]
            }
            tags = []
            for i in np.argsort(-np.where(categories == 4, probs, -1))[:3]:
                if probs[i] < TAGGER_THRESHOLD:
                    break
                tag = library.alias.get(str(names[i]), str(names[i]))
                tags.append(
                    Candidate(
                        tag,
                        "tagger",
                        normalize(float(probs[i]), TAGGER_THRESHOLD, 1.0),
                        round(float(probs[i]), 4),
                        library.work(tag),
                    )
                )
            ccip = cls._session("ccip", CHAR_DIR / "ccip.onnx")
            feature = ccip.run(["output"], {"input": _ccip_input(image)})[0][0]
            feature = feature / np.linalg.norm(feature)
            return {"tagger": tags, "library": library.match(feature, general)}

    @classmethod
    async def recognize(cls, data: bytes, tier: str) -> dict[str, list[Candidate]]:
        """用本地打标模型与角色库识别图片

        参数:
            data: 图片
            tier: lite / full

        返回:
            dict[str, list[Candidate]]: 来源 -> 候选（tagger / library）
        """
        if not cls.ready(tier):
            raise RuntimeError(f"{tier} 档模型尚未安装")
        try:
            return await asyncio.to_thread(cls._run, data, tier)
        finally:
            if cls._release:
                cls._release.cancel()
            cls._release = asyncio.get_running_loop().call_later(
                IDLE_SECONDS, cls._idle
            )
