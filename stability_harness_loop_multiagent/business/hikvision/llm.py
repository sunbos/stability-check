"""OpenAI-compatible LLM client using stdlib urllib only (zero deps).

Mirrors master branch tests/harness/llm_client.py. Defaults to OpenRouter
free model tencent/hy3:free. API key read from env LLM_API_KEY /
OPENROUTER_API_KEY or repo-root .env (auto-loaded, never overwrites
existing env). When no key is available, get_client() returns None and
callers fall back to rule-based logic.
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
    """Load .env into os.environ without overriding existing vars."""
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    if path is None:
        here = os.path.dirname(os.path.abspath(__file__))
        # business/hikvision/llm.py -> repo root: up 3 levels
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
    """Read LLM API key: LLM_API_KEY > OPENROUTER_API_KEY > .env."""
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
    """Build client; return None if no key (caller falls back to rules)."""
    key = _api_key()
    if not key:
        return None
    model = os.environ.get("LLM_MODEL") or os.environ.get(
        "OPENROUTER_MODEL", DEFAULT_MODEL)
    base_url = os.environ.get("LLM_BASE_URL") or os.environ.get(
        "OPENROUTER_BASE_URL", DEFAULT_BASE_URL)
    return OpenAICompatibleClient(api_key=key, model=model, base_url=base_url)


class OpenAICompatibleClient:
    """Minimal OpenAI-compatible chat-completions client (stdlib only)."""

    def __init__(self, api_key: str, model: str, base_url: str) -> None:
        self.api_key = api_key  # in-memory only, never logged
        self.model = model
        self.base_url = base_url.rstrip("/")

    def chat(self, system_prompt: str, user_prompt: str,
             timeout: float = 30.0) -> str | None:
        """Return model text; None on failure (caller degrades to rules)."""
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
        """chat() + extract first JSON object; None on failure."""
        text = self.chat(system_prompt, user_prompt, timeout=timeout)
        if not text:
            return None
        return _extract_first_json(text)


def _extract_first_json(text: str) -> dict | None:
    """Extract first JSON object from text (tolerant of nested braces)."""
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
