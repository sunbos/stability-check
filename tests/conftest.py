"""pytest 全局配置：动态注册 marker（1 marker = 1 用例）+ 给 test_stability_scenario 自动打 marker。

3 维 marker（与 spec §3 用例组织方式一致）：
- scenario_id（如 Stability_0001）：唯一用例标识
- category（如 重启稳定性）：类别筛选
- level（如 L2）：等级筛选

用法：
    pytest -m Stability_0001              # 跑单条用例
    pytest -m "重启稳定性"                 # 跑类别下所有用例
    pytest -m "L2"                        # 跑等级 L2 所有用例
    pytest -m "重启稳定性 and L2"          # 组合筛选

注：模块级扫描 configs/stability_*.yaml 在 import 时即填充 SCENARIO_MAP /
SCENARIO_IDS / SCENARIO_META，供 test_stability_scenario.py 的 parametrize
直接读取（避免 `from .conftest import` 相对导入问题——tests/ 无 __init__.py，
conftest 作为特殊模块被 pytest 加入 sys.path，可直接 `from conftest import`）。
"""
from pathlib import Path

import pytest
import yaml

# 加载项目根的 .env（如果存在），让 pytest 能读到 HIK_HOST / LLM_API_KEY 等
# 真实环境变量。缺失不报错（CI 环境可能无 .env，走 skip 路径）。
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass  # python-dotenv 未安装时跳过，依赖系统环境变量

# configs/ 目录（项目根/configs，conftest.py 在 tests/ 下，parent.parent 即项目根）
_CONFIGS_DIR = Path(__file__).parent.parent / "configs"

# scenario_id -> yaml_path（供 test_stability_scenario 读取路径）
SCENARIO_MAP: dict[str, Path] = {}
# scenario_id 有序列表（供 parametrize 遍历，sorted 保证顺序稳定）
SCENARIO_IDS: list[str] = []
# scenario_id -> {category, level, name}（供 pytest_collection_modifyitems 打 marker）
SCENARIO_META: dict[str, dict[str, str]] = {}


def _scan_scenarios() -> None:
    """模块级扫描：填充 SCENARIO_MAP / SCENARIO_IDS / SCENARIO_META。

    扫描 configs/stability_*.yaml（非 stability_ 前缀的 yaml 不算用例，如
    scenario_template.yaml / door_restart_stability.yaml 历史文件）。
    损坏的 yaml 跳过，不阻塞 pytest 收集（容错优先）。
    """
    if not _CONFIGS_DIR.exists():
        return
    for yaml_path in sorted(_CONFIGS_DIR.glob("stability_*.yaml")):
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 - 损坏的 yaml 跳过，不阻塞 pytest 收集
            continue
        if not isinstance(data, dict):
            continue
        sid = data.get("id")
        if not sid:
            continue
        SCENARIO_MAP[sid] = yaml_path
        SCENARIO_IDS.append(sid)
        SCENARIO_META[sid] = {
            "category": str(data.get("category", "") or ""),
            "level": str(data.get("level", "") or ""),
            "name": str(data.get("name", "") or ""),
        }


# 模块级执行：import conftest 时即填充（pytest 收集 test 文件前 conftest 已加载）
_scan_scenarios()


def pytest_configure(config):
    """注册 3 维 marker：scenario_id / category / level。

    在 pytest 收集前注册，避免 `-m Stability_0001` 因未知 marker 报 warning。
    """
    for sid in SCENARIO_IDS:
        config.addinivalue_line("markers", f"{sid}: 稳定性用例 {sid}")
        meta = SCENARIO_META.get(sid, {})
        if meta.get("category"):
            config.addinivalue_line(
                "markers", f"{meta['category']}: 类别 {meta['category']}"
            )
        if meta.get("level"):
            config.addinivalue_line(
                "markers", f"{meta['level']}: 等级 {meta['level']}"
            )


def pytest_collection_modifyitems(config, items):
    """给 test_stability_scenario 的每个 callspec 自动打上对应 marker。

    parametrize 生成的 callspec 名形如 ``test_stability_scenario[Stability_0001]``，
    item.name 是 callspec 级（含参数），用 startswith 匹配保证健壮性。
    """
    for item in items:
        if not item.name.startswith("test_stability_scenario"):
            continue
        callspec = getattr(item, "callspec", None)
        if not callspec or "scenario_id" not in callspec.params:
            continue
        sid = callspec.params["scenario_id"]
        meta = SCENARIO_META.get(sid, {})
        item.add_marker(getattr(pytest.mark, sid))
        if meta.get("category"):
            item.add_marker(getattr(pytest.mark, meta["category"]))
        if meta.get("level"):
            item.add_marker(getattr(pytest.mark, meta["level"]))
