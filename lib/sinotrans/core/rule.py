
from sinotrans.utils.logger import Logger
import re

class Rule:
    DEFAULT_SPLITTER = '#'
    """
    字段映射规则类，对应单个字段的映射规则
    """
    field_name: str
    splitter: str
    index: int
    mode: str
    method: str
    dir: str
    dp: int
    count: int
    readingMode: str
    considerEmpty: bool
    def __init__(self, field_name = None, splitter = None, index = None, mode = None, method = None, dp = 2, dir = "row", count = None, readingMode = None, considerEmpty=False) -> None:
        self.field_name = field_name
        self.splitter = splitter
        self.index = index
        self.mode = mode
        self.method = method
        self.dp = dp
        self.dir = dir
        self.count = count
        self.readingMode = readingMode
        self.considerEmpty = considerEmpty

    @classmethod
    def get_Map_Dict_From_List(cls, rule_list:list['Rule'], value):
        """应用字段映射规则"""
        return {rule.field_name: value for rule in rule_list}
    def map_action(self, raw_value, readUntilBlank_index = None):
        """
        根据Rule对象，处理值
        readUntilBlank_inde是为程序动态迭代readUntilBlank的序列值准备的
        """
        if not raw_value:
            return None
        processed_value = raw_value
        # 开发者动态迭代读取
        if readUntilBlank_index is not None and self.readingMode == "readUntilBlank":
            processed_value = processed_value.split(self.DEFAULT_SPLITTER)[readUntilBlank_index]
        # 字段分割处理
        if self.splitter:
            split_values = raw_value.split(self.splitter)# re.split(self.splitter, raw_value)
            # 模式
            if self.mode == "last":
                processed_value = split_values[-1] if split_values else ""
            elif self.mode == "allbutlast":
                processed_value = self.splitter.join(split_values[:-1]) if len(split_values) > 1 else ""
            # 索引优先级更高
            if self.index is not None:
                try:
                    if not (self.index <= len(split_values) and self.index > 0):
                        raise ValueError(f"请检查index值：{self.index}")
                    processed_value = split_values[self.index-1]
                except IndexError:
                    Logger.error(f"❌ 字段分割失败: {self.field_name}={raw_value}")
                    processed_value = ""
                    raise ValueError(f"❌ 字段分割失败: {self.field_name}={raw_value}")
        # 四舍五入处理
        if self.method == 'round':
            try:
                num_value = float(processed_value)
                processed_value = round(num_value, self.dp)
            except ValueError:
                Logger.error(f"❌ 四舍五入失败: 值 '{processed_value}' 不是有效数字")
                processed_value = ""
                raise ValueError(f"❌ 四舍五入失败: 值 '{processed_value}' 不是有效数字")

        return processed_value