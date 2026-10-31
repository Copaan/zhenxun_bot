"""知识库检索：分词、文本切分与内存索引

向量为归一化的 float32，按库加载为矩阵用 numpy 计算余弦相似度；
全文检索用 jieba 分词的倒排 BM25，归一化后按权重与余弦相似度相加。
"""

from dataclasses import dataclass
import re

import jieba
import numpy as np

BM25_WEIGHT = 0.1
"""BM25 归一化分数在融合分中的权重"""
SEPARATORS = ["\n\n", "\n", "。", "！", "？", "；", "，", " ", ""]
"""切分文本时依次尝试的分隔符"""


def tokenize(text: str) -> list[str]:
    """全文检索分词

    参数:
        text: 文本

    返回:
        list[str]: 词列表
    """
    return [
        w
        for w in jieba.lcut_for_search(text.lower())
        if w.strip() and not re.fullmatch(r"\W+", w)
    ]


def split_text(text: str, size: int, overlap: int) -> list[str]:
    """按段落、句子、标点递归切分文本，相邻分块重叠 overlap 字

    参数:
        text: 文本
        size: 分块大小
        overlap: 重叠字数

    返回:
        list[str]: 分块
    """
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if not text:
        return []
    overlap = max(0, min(overlap, size // 2))

    def merge(parts: list[str], sep: str) -> list[str]:
        chunks, current, length = [], [], 0
        for part in parts:
            extra = len(sep) if current else 0
            if current and length + extra + len(part) > size:
                chunks.append(sep.join(current))
                while current and (length > overlap or length + len(part) > size):
                    length -= len(current.pop(0)) + (len(sep) if current else 0)
                extra = len(sep) if current else 0
            current.append(part)
            length += extra + len(part)
        if current:
            chunks.append(sep.join(current))
        return chunks

    def split(value: str, seps: list[str]) -> list[str]:
        sep = next((s for s in seps if s == "" or s in value), "")
        rest = seps[seps.index(sep) + 1 :] if sep else []
        parts = value.split(sep) if sep else list(value)
        out, good = [], []
        for part in parts:
            if not part:
                continue
            if len(part) <= size:
                good.append(part)
                continue
            if good:
                out += merge(good, sep)
                good = []
            if rest:
                out += split(part, rest)
            else:
                out += [part[i : i + size] for i in range(0, len(part), size)]
        if good:
            out += merge(good, sep)
        return out

    if len(text) <= size:
        return [text]
    return [c.strip() for c in split(text, SEPARATORS) if c.strip()]


def to_blob(vector: np.ndarray) -> bytes:
    """归一化后转为 float32 字节"""
    vector = np.asarray(vector, dtype=np.float32)
    norm = float(np.linalg.norm(vector))
    return (vector / norm if norm else vector).tobytes()


@dataclass
class Hit:
    chunk_id: int
    """分块id"""
    kb_id: int
    """所属知识库"""
    doc_id: int
    """所属文档"""
    content: str
    """内容"""
    score: float
    """融合分"""
    image_path: str | None = None
    """图片路径"""


class _Index:
    """单个知识库的内存索引"""

    def __init__(self, kb_id: int, rows: list[tuple]):
        self.kb_id = kb_id
        self.ids = [r[0] for r in rows]
        self.doc_ids = [r[1] for r in rows]
        self.contents = [r[2] for r in rows]
        self.images = [r[6] for r in rows]
        vectors = [
            np.frombuffer(r[4], dtype=np.float32) if r[4] else None for r in rows
        ]
        self.dim = next((len(v) for v in vectors if v is not None), 0)
        self.vectors = np.zeros((len(rows), self.dim), dtype=np.float32)
        for i, v in enumerate(vectors):
            if v is not None and len(v) == self.dim:
                self.vectors[i] = v
        self.image_vectors = None
        self.has_image = np.array([bool(r[5]) for r in rows], dtype=bool)
        if self.has_image.any():
            self.image_vectors = np.zeros_like(self.vectors)
            for i, r in enumerate(rows):
                if r[5] and len(blob := np.frombuffer(r[5], np.float32)) == self.dim:
                    self.image_vectors[i] = blob
        self._build_bm25([str(r[3] or "").split() for r in rows])

    def _build_bm25(self, docs: list[list[str]], k1: float = 1.5, b: float = 0.75):
        """构建 CSR 倒排的 BM25 权重"""
        vocab: dict[str, int] = {}
        rows, cols, tfs = [], [], []
        lengths = np.array([len(d) for d in docs] or [0], dtype=np.float32)
        for d, words in enumerate(docs):
            counts: dict[int, int] = {}
            for w in words:
                tid = vocab.setdefault(w, len(vocab))
                counts[tid] = counts.get(tid, 0) + 1
            rows += counts.keys()
            cols += [d] * len(counts)
            tfs += counts.values()
        term = np.array(rows, dtype=np.int64)
        order = np.argsort(term, kind="stable")
        term = term[order]
        self.doc = np.array(cols, dtype=np.int64)[order]
        tf = np.array(tfs, dtype=np.float32)[order]
        self.indptr = np.zeros(len(vocab) + 1, dtype=np.int64)
        np.add.at(self.indptr, term + 1, 1)
        self.indptr = np.cumsum(self.indptr)
        df = np.diff(self.indptr).astype(np.float32)
        n = len(docs)
        idf = np.log(1 + (n - df + 0.5) / (df + 0.5))
        norm = k1 * (1 - b + b * lengths / max(float(lengths.mean()), 1.0))
        self.weights = (
            idf[term] * tf * (k1 + 1) / (tf + norm[self.doc] if n else tf)
        ).astype(np.float32)
        self.vocab = vocab

    def search(
        self, query: np.ndarray | None, words: list[str], k: int
    ) -> list[tuple[int, float]]:
        """检索

        参数:
            query: 归一化查询向量，为空时只用全文检索
            words: 查询分词
            k: 返回条数

        返回:
            list[tuple[int, float]]: (行号, 融合分)

        异常:
            ValueError: 查询向量维度与索引不一致
        """
        n = len(self.ids)
        if not n:
            return []
        score = np.zeros(n, dtype=np.float32)
        if query is not None and self.dim:
            if len(query) != self.dim:
                raise ValueError(
                    "知识库向量与当前 embedding 模型的维度不一致，请在 WebUI 重建。"
                )
            score = self.vectors @ query
            if self.image_vectors is not None:
                score = np.where(
                    self.has_image, (score + self.image_vectors @ query) / 2, score
                )
        bm25 = np.zeros(n, dtype=np.float32)
        for tid in {self.vocab[w] for w in words if w in self.vocab}:
            part = slice(self.indptr[tid], self.indptr[tid + 1])
            np.add.at(bm25, self.doc[part], self.weights[part])
        if bm25.max() > 0:
            score = score + BM25_WEIGHT * bm25 / bm25.max()
        k = min(k, n)
        top = np.argpartition(-score, k - 1)[:k]
        top = top[np.argsort(-score[top])]
        return [(int(i), float(score[i])) for i in top if score[i] > 0]
