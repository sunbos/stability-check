"""HikvisionClient 单元测试(纯逻辑,不涉及真实 HTTP)。

验证 client 构造、URL 拼接、超时配置,不验证 HTTP 行为(HTTP 行为由真机测试覆盖)。
这些测试在 PR1 Task 1.4 改造 client.py 后应全部通过(TDD green phase)。
"""
import pytest
from stability_harness_loop_multiagent.business.hikvision.client import HikvisionClient


def test_client_construction_with_default_port():
    """构造:默认端口 80,base_url 拼接正确"""
    client = HikvisionClient(host="192.168.3.33", username="admin", password="pass")
    assert client._client.base_url == "http://192.168.3.33:80"


def test_client_construction_with_custom_port_and_timeout():
    """构造:自定义端口和超时"""
    client = HikvisionClient(host="10.0.0.1", port=8080, username="admin",
                              password="pass", timeout=10.0)
    assert client._client.base_url == "http://10.0.0.1:8080"
    assert client._client.timeout.connect == 10.0


def test_client_has_thread_lock_for_digest_auth():
    """构造:必须有线程锁保护 Digest Auth state(并发 query_events 用)"""
    client = HikvisionClient(host="x", username="x", password="x")
    assert hasattr(client, "_lock")
    assert client._lock is not None
