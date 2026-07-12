"""重启代理：对设备发起远程重启。

仅使用标准库，不引入第三方依赖，不执行任何实际网络请求（网络行为由传入的
DeviceClient 封装）。模块只定义函数，供上层工作流调用。
"""

from __future__ import annotations


def run_reboot(client) -> None:
    """调用 client.reboot() 发起设备重启。

    任何异常（如 RuntimeError / 网络错误等）均直接向上抛出，不做捕获或吞掉，
    交由上层工作流统一处理。
    """
    client.reboot()
