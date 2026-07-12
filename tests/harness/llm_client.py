"""OpenRouter LLM 客户端（仅使用标准库 urllib / json / os）。

设计约束（安全）
--------------
* API key **只**从环境变量或仓库根目录的 `.env` 读取，**绝不**硬编码、绝不打印。
* 当没有可用 key 时，`get_client()` 返回 None，调用方据此走规则降级（rule-based）。
* 模型默认 `tencent/hy3:free`（由用户授权提供），可通过 `OPENROUTER_MODEL` 覆盖。
* 所有请求统一 30s 超时，模型输出做防御式 JSON 解析，解析失败返回 None。

仅依赖标准库，无第三方依赖。本模块不直接 import bus / agent（保持纯粹、可单测）。
"""

from __future__ import annotations

import json
import os
import urllib.request
import urllib.error

# 默认模型（用户提供的 OpenRouter 模型）。
DEFAULT_MODEL = "tencent/hy3:free"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"

# 读取 .env 的最小实现，避免引入 python-dotenv 第三方依赖。
# 仅在“环境变量中尚未设置”时才用 .env 中的值填充，从不覆盖已有环境变量。
_DOTENV_LOADED = False


def _load_dotenv(path: str | None = None) -> None:
    """把 .env 中的键值塞进 os.environ（不覆盖已存在的变量）。

    仅读取 KEY=VALUE 行（忽略空行与 # 注释），不做 shell 展开。
    """
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    if path is None:
        # 仓库根目录：tests/harness/llm_client.py -> 上两级
        here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(os.path.dirname(os.path.dirname(here)), ".env")
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip()
                # 去掉可选引号
                if len(val) >= 2 and val[0] in ("'", '"') and val[-1] == val[0]:
                    val = val[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = val
    except OSError:
        # .env 读取失败不应阻断整场拷机（降级为无 LLM）。
        return


def _api_key() -> str | None:
    """读取 OpenRouter key：优先环境变量，其次 .env。返回 None 表示无 key。"""
    key = os.environ.get("OPENROUTER_API_KEY")
    if key:
        return key
    _load_dotenv()
    return os.environ.get("OPENROUTER_API_KEY") or None


def get_client() -> "OpenRouterClient | None":
    """构造客户端；无 key 时返回 None（调用方走规则降级）。

    注意：key 永远不被打印、不被写入日志。
    """
    key = _api_key()
    if not key:
        return None
    return OpenRouterClient(
        api_key=key,
        model=os.environ.get("OPENROUTER_MODEL", DEFAULT_MODEL),
        base_url=os.environ.get("OPENROUTER_BASE_URL", DEFAULT_BASE_URL),
    )


class OpenRouterClient:
    """极简 OpenRouter chat-completions 客户端（stdlib 实现）。"""

    def __init__(self, api_key: str, model: str, base_url: str) -> None:
        self.api_key = api_key  # 仅保存在内存，绝不外泄
        self.model = model
        self.base_url = base_url.rstrip("/")

    def chat(self, system_prompt: str, user_prompt: str, timeout: float = 30.0) -> str | None:
        """发起一次对话补全，返回模型文本；失败返回 None。

        不做流式；tencent/hy3:free 不支持 response_format=json，故要求模型在
        文本中返回 JSON，调用方自行解析。
        """
        url = self.base_url + "/chat/completions"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0,
            "max_tokens": 512,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {self.api_key}")
        req.add_header("HTTP-Referer", "burnin-framework")
        req.add_header("X-Title", "burnin-analyst")

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", "replace")
            data = json.loads(body)
            choices = data.get("choices") or []
            if not choices:
                return None
            return choices[0].get("message", {}).get("content")
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, OSError):
            # 网络/解析失败：返回 None，由调用方降级到规则。
            return None

    def chat_json(self, system_prompt: str, user_prompt: str, timeout: float = 30.0) -> dict | None:
        """chat() 的便捷封装：从模型文本中解析首个 JSON 对象，失败返回 None。"""
        text = self.chat(system_prompt, user_prompt, timeout=timeout)
        if not text:
            return None
        return _extract_first_json(text)


def _extract_first_json(text: str) -> dict | None:
    """从文本中提取第一个 JSON 对象。宽松匹配 { ... }（含嵌套）。"""
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                snippet = text[start : i + 1]
                try:
                    return json.loads(snippet)
                except ValueError:
                    return None
    return None
