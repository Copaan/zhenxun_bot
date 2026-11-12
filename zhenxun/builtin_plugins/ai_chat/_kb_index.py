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
    if size <= 0:
        raise ValueError("分块大小必须大于零")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    overlap = max(0, min(overlap, size // 2))
    chunks, start = [], 0
    while start < len(text):
        end = min(start + size, len(text))
        if end < len(text):
            for separator in SEPARATORS[:-1]:
                split = text.rfind(separator, start + max(overlap + 1, size // 2), end)
                if split >= 0:
                    end = split + len(separator)
                    break
        chunk = text[start:end]
        if chunk.strip():
            chunks.append(chunk)
        if end == len(text):
            break
        start = max(start + 1, end - overlap)
    return chunks


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
        self.idf = idf
        self.unseen_idf = float(np.log(1 + max(n - 0.5, 0) / 1.5))

    def scores(
        self, query: np.ndarray | None, words: list[str]
    ) -> tuple[np.ndarray, np.ndarray]:
        """计算每行的余弦相似度与归一化 BM25 分

        参数:
            query: 归一化查询向量，为空时余弦相似度全为 0
            words: 查询分词

        返回:
            tuple[np.ndarray, np.ndarray]: 余弦相似度, 0~1 的 BM25 分

        异常:
            ValueError: 查询向量维度与索引不一致
        """
        n = len(self.ids)
        cosine = np.zeros(n, dtype=np.float32)
        if query is not None and self.dim and n:
            if len(query) != self.dim:
                raise ValueError(
                    "知识库向量与当前 embedding 模型的维度不一致，请在 WebUI 重建。"
                )
            cosine = self.vectors @ query
            if self.image_vectors is not None:
                cosine = np.where(
                    self.has_image, (cosine + self.image_vectors @ query) / 2, cosine
                )
        bm25 = np.zeros(n, dtype=np.float32)
        for tid in {self.vocab[w] for w in words if w in self.vocab}:
            part = slice(self.indptr[tid], self.indptr[tid + 1])
            np.add.at(bm25, self.doc[part], self.weights[part])
        if n and bm25.max() > 0:
            bm25 = bm25 / bm25.max()
        return cosine, bm25

    def coverage(self, words: list[str]) -> np.ndarray:
        """每行命中的查询词按 idf 加权占全部查询词的比例，没出现在索引中的词
        按只出现在一行计算

        参数:
            words: 查询分词

        返回:
            np.ndarray: 0~1 的覆盖度
        """
        n = len(self.ids)
        hit = np.zeros(n, dtype=np.float32)
        terms = set(words)
        if not n or not terms:
            return hit
        total = 0.0
        for w in terms:
            if w not in self.vocab:
                total += self.unseen_idf
                continue
            tid = self.vocab[w]
            total += float(self.idf[tid])
            hit[self.doc[self.indptr[tid] : self.indptr[tid + 1]]] += self.idf[tid]
        return hit / total if total > 0 else hit

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
        cosine, bm25 = self.scores(query, words)
        score = cosine + BM25_WEIGHT * bm25
        k = min(k, n)
        top = np.argpartition(-score, k - 1)[:k]
        top = top[np.argsort(-score[top])]
        return [(int(i), float(score[i])) for i in top if score[i] > 0]
