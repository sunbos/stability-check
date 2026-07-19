"""兼容 OpenAI 的大模型客户端，仅使用标准库 urllib（零依赖）。

对应主干分支 tests/harness/llm_client.py。默认使用 OpenRouter 免费模型
tencent/hy3:free。API 密钥从环境变量 LLM_API_KEY / OPENROUTER_API_KEY 或
仓库根目录的 .env 读取（自动加载，且永不覆盖已有环境变量）。当未配置密钥时，
get_client() 返回 None，调用方回退到规则逻辑。
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

DEFAULT_MODEL = "tencent/hy3:free"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"

_DOTENV_LOADED = False


def _load_dotenv(path: str | None = None) -> None:
    """将 .env 加载进 os.environ，但不覆盖已有变量。"""
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    if path is None:
        here = os.path.dirname(os.path.abspath(__file__))
        # business/hikvision/llm.py -> 仓库根目录：向上 3 层
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(here))), ".env")
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
                if len(val) >= 2 and val[0] in ("'", '"') and val[-1] == val[0]:
                    val = val[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = val
    except OSError:
        return


def _api_key() -> str | None:
    """读取 LLM API 密钥：LLM_API_KEY > OPENROUTER_API_KEY > .env。"""
    key = os.environ.get("LLM_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if key:
        return key
    _load_dotenv()
    return (
        os.environ.get("LLM_API_KEY")
        or os.environ.get("OPENROUTER_API_KEY")
        or None
    )


def get_client() -> "OpenAICompatibleClient | None":
    """构造客户端；若无密钥则返回 None（调用方回退到规则逻辑）。"""
    key = _api_key()
    if not key:
        return None
    model = os.environ.get("LLM_MODEL") or os.environ.get(
        "OPENROUTER_MODEL", DEFAULT_MODEL)
    base_url = os.environ.get("LLM_BASE_URL") or os.environ.get(
        "OPENROUTER_BASE_URL", DEFAULT_BASE_URL)
    return OpenAICompatibleClient(api_key=key, model=model, base_url=base_url)


class OpenAICompatibleClient:
    """极简的、兼容 OpenAI 的对话补全客户端（仅标准库）。"""

    def __init__(self, api_key: str, model: str, base_url: str) -> None:
        self.api_key = api_key  # 仅存内存，绝不打印日志
        self.model = model
        self.base_url = base_url.rstrip("/")

    def chat(self, system_prompt: str, user_prompt: str,
             timeout: float = 30.0) -> str | None:
        """返回模型文本；失败时返回 None（调用方回退到规则逻辑）。"""
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
        req.add_header("HTTP-Referer", "stability-harness-hikvision")
        req.add_header("X-Title", "hikvision-advisor")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", "replace")
            data = json.loads(body)
            choices = data.get("choices") or []
            if not choices:
                return None
            return choices[0].get("message", {}).get("content")
        except (urllib.error.URLError, urllib.error.HTTPError,
                ValueError, OSError):
            return None

    def chat_json(self, system_prompt: str, user_prompt: str,
                  timeout: float = 30.0) -> dict | None:
        """chat() + 抽取第一个 JSON 对象；失败时返回 None。"""
        text = self.chat(system_prompt, user_prompt, timeout=timeout)
        if not text:
            return None
        return _extract_first_json(text)


def _extract_first_json(text: str) -> dict | None:
    """从文本中抽取第一个 JSON 对象（可容忍嵌套花括号）。"""
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
                snippet = text[start: i + 1]
                try:
                    return json.loads(snippet)
                except ValueError:
                    return None
    return None


__all__ = ["OpenAICompatibleClient", "get_client"]
