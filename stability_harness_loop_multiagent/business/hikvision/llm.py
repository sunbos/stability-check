"""兼容 OpenAI 的大模型客户端（openai SDK + OpenRouter）。

用 openai SDK 替换手写 urllib，用 pydantic structured output 替换手写 JSON 抽取。
默认使用 OpenRouter 免费模型 tencent/hy3:free。API 密钥从环境变量
LLM_API_KEY / OPENROUTER_API_KEY 或仓库根目录的 .env 读取（自动加载，且永不
覆盖已有环境变量）。当未配置密钥时，get_client() 返回 None，调用方回退到
规则逻辑。

兼容说明：保留 ``OpenAICompatibleClient`` 类与 ``.chat`` / ``.chat_json`` /
``.model`` 接口，使 runner.py / hikvision_real_env.py 零改动；``.chat_json``
新增 ``response_model`` 参数支持 structured output。另提供模块级 ``chat_json``
函数作为新接口（供 advisor.py Task 2.3 等新代码使用）。
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

from openai import OpenAI
from pydantic import BaseModel

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "tencent/hy3:free"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"

_DOTENV_LOADED = False


def _load_dotenv(path: str | None = None) -> None:
    """将 .env 加载进 os.environ，但不覆盖已有变量（保留以避免引入 python-dotenv）。"""
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


def _extract_first_json(text: str) -> dict | None:
    """从文本中抽取第一个 JSON 对象（可容忍嵌套花括号）。

    作为无 ``response_model`` 时的回退路径（向后兼容 runner.py /
    hikvision_real_env.py 的旧调用方）；新代码应优先用 structured output
    （``response_model`` 参数）。
    """
    if not text:
        return None
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


class OpenAICompatibleClient:
    """薄 wrapper：用 openai SDK 替代手写 urllib，保持原 ``.chat`` / ``.chat_json``
    / ``.model`` 接口。

    新增 ``response_model`` 参数支持 pydantic structured output
    （``client.beta.chat.completions.parse``），优于手写 JSON 抽取。
    """

    def __init__(self, api_key: str, model: str, base_url: str) -> None:
        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url.rstrip("/"),
            default_headers={
                "HTTP-Referer": "stability-harness-hikvision",
                "X-Title": "hikvision-advisor",
            },
        )
        self.model = model  # 向后兼容（hikvision_real_env.py 引用 llm.model）

    def chat(self, system_prompt: str, user_prompt: str,
             timeout: float = 30.0) -> str | None:
        """返回模型文本；失败时返回 None（调用方回退到规则逻辑）。"""
        try:
            completion = self._client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                max_tokens=512,
                timeout=timeout,
            )
            return completion.choices[0].message.content
        except Exception as exc:  # noqa: BLE001 - 失败一律回退规则
            logger.warning("LLM chat 调用失败，回退规则兜底: %s", exc)
            return None

    def chat_json(self, system_prompt: str, user_prompt: str,
                  timeout: float = 30.0,
                  response_model: Optional[type[BaseModel]] = None) -> dict | None:
        """chat + JSON 抽取；失败时返回 None。

        - ``response_model`` 非 None：用 openai structured output（pydantic 校验），
          返回 ``model_dump()``
        - ``response_model`` 为 None：普通 chat completion，文本经
          ``_extract_first_json`` 抽取（向后兼容旧调用方）
        """
        if response_model is not None:
            try:
                completion = self._client.beta.chat.completions.parse(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format=response_model,
                    timeout=timeout,
                )
                parsed = completion.choices[0].message.parsed
                return parsed.model_dump() if parsed else None
            except Exception as exc:  # noqa: BLE001 - 失败一律回退规则
                logger.warning("LLM structured output 调用失败，回退规则兜底: %s", exc)
                return None
        # 无 response_model：走普通 chat，再抽取 JSON（向后兼容旧调用方）
        text = self.chat(system_prompt, user_prompt, timeout=timeout)
        if not text:
            return None
        return _extract_first_json(text)


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


def chat_json(client, system_prompt: str, user_prompt: str,
              response_model: Optional[type[BaseModel]] = None) -> Optional[dict]:
    """模块级 ``chat_json``（新接口，供 advisor.py Task 2.3 等新代码使用）。

    - ``client`` 为 None：返回 None（调用方回退规则兜底）
    - ``client`` 是 ``OpenAICompatibleClient``：委托其 ``.chat_json(response_model=...)``
    - ``client`` 是原生 ``OpenAI``：按 openai SDK 直接调用（通用兜底）
    """
    if client is None:
        return None
    if isinstance(client, OpenAICompatibleClient):
        return client.chat_json(system_prompt, user_prompt,
                                response_model=response_model)
    # 原生 OpenAI 客户端路径（get_client() 不会返回此类型，留作通用兜底）
    model = os.environ.get("LLM_MODEL", DEFAULT_MODEL)
    try:
        if response_model is not None:
            completion = client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format=response_model,
            )
            parsed = completion.choices[0].message.parsed
            return parsed.model_dump() if parsed else None
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=512,
        )
        return _extract_first_json(completion.choices[0].message.content or "")
    except Exception as exc:  # noqa: BLE001 - 失败一律回退规则
        logger.warning("LLM 调用失败，回退规则兜底: %s", exc)
        return None


__all__ = ["OpenAICompatibleClient", "get_client", "chat_json",
           "DEFAULT_MODEL", "DEFAULT_BASE_URL"]
