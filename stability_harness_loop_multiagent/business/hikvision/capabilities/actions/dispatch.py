# business/hikvision/capabilities/actions/dispatch.py
"""DispatchAction —— 通用配置下发(下发稳定性用例 0061-0078)。

通过 endpoint + method + body 三参数泛化任意 ISAPI PUT/POST/GET 请求,
适配海康门禁各种配置下发场景(开门策略/卡片/权限/参数等)。
"""
import logging
from typing import Any

from .base import ActionResult

logger = logging.getLogger(__name__)


class DispatchAction:
    """通用下发 Action。

    Args:
        endpoint: ISAPI 路径(如 /ISAPI/AccessControl/UserInfo/setup)
        method: HTTP 方法(默认 PUT)
        body: 请求体 dict(可含 _raw 字符串则直接当 XML/JSON 文本发送)
    """

    def __init__(self, endpoint: str,
                 method: str = "PUT",
                 body: dict | None = None,
                 **_: Any) -> None:
        self._endpoint = endpoint
        self._method = method
        self._body = body or {}

    def execute(self, ctx: Any) -> ActionResult:
        """执行下发。"""
        try:
            ctx.client.request_json(self._method, self._endpoint, body=self._body)
            return ActionResult(ok=True, data={"endpoint": self._endpoint})
        except Exception as exc:  # noqa: BLE001
            logger.warning("DispatchAction 失败(endpoint=%s): %s",
                           self._endpoint, exc)
            return ActionResult(ok=False, error=str(exc))


__all__ = ["DispatchAction"]
