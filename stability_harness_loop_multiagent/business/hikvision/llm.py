"""LLM 客户端(openai SDK + OpenRouter)。

用 openai SDK + pydantic structured output 替换手写 urllib + JSON 抽取。
无 LLM_API_KEY 时 get_client() 返回 None,调用方回退规则兜底。
"""
import json
import logging
import os
import re
from openai import OpenAI
from pydantic import BaseModel

logger = logging.getLogger(__name__)
DEFAULT_MODEL = "tencent/hy3:free"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"
_DOTENV_LOADED = False


def _load_dotenv() -> None:
    """极简 .env 加载(不覆盖已有变量;保留以避免引入 python-dotenv)。"""
    global _DOTENV_LOADED
    if _DOTENV_LOADED: return
    _DOTENV_LOADED = True
    here = os.path.dirname(os.path.abspath(__file__))
    env = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(here))), ".env")
    if not os.path.exists(env):
        return
    try:
        with open(env, encoding="utf-8") as fh:
            for line in fh:
                s = line.strip()
                if s and not s.startswith("#") and "=" in s:
                    k, _, v = s.partition("=")
                    v = v.strip()
                    if len(v) >= 2 and v[0] in "'\"" and v[-1] == v[0]:
                        v = v[1:-1]
                    k = k.strip()
                    if k and k not in os.environ:
                        os.environ[k] = v
    except OSError:
        return


def get_client() -> OpenAI | None:
    """构造 OpenAI 客户端;无密钥返回 None(调用方回退规则兜底)。"""
    key = os.environ.get("LLM_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not key:
        _load_dotenv()
        key = os.environ.get("LLM_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not key:
        return None
    base_url = os.environ.get("LLM_BASE_URL") or os.environ.get(
        "OPENROUTER_BASE_URL", DEFAULT_BASE_URL)
    return OpenAI(api_key=key, base_url=base_url.rstrip("/"), default_headers={
        "HTTP-Referer": "stability-harness-hikvision", "X-Title": "hikvision-advisor"})


def get_model_name() -> str:
    """返回最终生效的 LLM 模型名(供显示与调用共用同一来源)。"""
    return os.environ.get("LLM_MODEL") or os.environ.get("OPENROUTER_MODEL") or DEFAULT_MODEL


def _extract_first_json(text: str) -> dict | None:
    """从 markdown 围栏 / prose 中抽取第一个 JSON 对象;失败返回 None。

    免费模型常返回 ```json ... ``` 包裹或前后带解释文字,直接 json.loads 会失败。
    依次尝试:直接解析 / ```json...``` 围栏 / 第一个 {...} 块。
    """
    if not text:
        return None
    candidates = [text]
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    if m:
        candidates.append(m.group(1))
    m = re.search(r"\{[^{}]*\}", text, re.S)
    if m:
        candidates.append(m.group(0))
    for c in candidates:
        try:
            v = json.loads(c)
            if isinstance(v, dict):
                return v
        except json.JSONDecodeError:
            continue
    return None


def chat_json(client: OpenAI | None, system_prompt: str, user_prompt: str,
              response_model: type[BaseModel] | None = None,
              timeout: float = 30.0) -> dict | None:
    """调用 LLM 返回 dict;失败返回 None(调用方回退规则兜底)。

    response_model 非 None 时用 structured output 返回 model_dump();为 None 时
    普通聊天返回,优先返回解析后的 dict,无法解析时返回 ``{"text": ...}``。
    """
    if client is None:
        return None
    model = get_model_name()
    msgs = [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]
    try:
        if response_model is not None:
            resp = client.beta.chat.completions.parse(
                model=model, messages=msgs, response_format=response_model, timeout=timeout)
            parsed = resp.choices[0].message.parsed
            return parsed.model_dump() if parsed else None
        resp = client.chat.completions.create(
            model=model, messages=msgs, temperature=0.0, max_tokens=512, timeout=timeout)
        text = resp.choices[0].message.content or ""
        return _extract_first_json(text) or {"text": text}
    except Exception as exc:  # noqa: BLE001 - 失败一律回退规则
        logger.warning("LLM 调用失败,回退规则兜底: %s", exc)
        return None


__all__ = ["get_client", "chat_json", "get_model_name", "DEFAULT_MODEL", "DEFAULT_BASE_URL"]
