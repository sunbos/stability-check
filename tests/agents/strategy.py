import re


class Strategy:
    """解析策略提示词文本，提供额外状态断言与日志原文。"""

    def __init__(self, text: str):
        # 保存为字符串，None 视为空
        self.text = text if isinstance(text, str) else ""

    def extra_status_asserts(self, round_no: int, consecutive_reboots: int) -> list:
        """返回 list of (field, expected) 额外状态断言。

        解析 text 中类似 "连续重启N次后断言 <field>=<value>" 的指令。
        当 consecutive_reboots >= N 时加入该断言。value 为整数则转 int，
        否则保留字符串。无法解析或 text 为空时返回 []。
        """
        result = []
        if not self.text:
            return result

        # 匹配：连续重启 N 次后断言 field=value
        pattern = re.compile(
            r"连续重启\s*(\d+)\s*次后断言\s*([^\s=]+)\s*=\s*(\S+)"
        )
        try:
            for m in pattern.finditer(self.text):
                n = int(m.group(1))
                field = m.group(2)
                raw_value = m.group(3)
                if consecutive_reboots >= n:
                    # 整数则转 int，否则保留字符串
                    try:
                        value = int(raw_value)
                    except ValueError:
                        value = raw_value
                    result.append((field, value))
        except Exception:
            return []

        return result

    def notes(self) -> str:
        """原样返回策略文本，供 supervisor 日志。"""
        return self.text
