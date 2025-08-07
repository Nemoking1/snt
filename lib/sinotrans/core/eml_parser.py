import os
from sinotrans.utils.logger import Logger
from sinotrans.utils.global_thread_pool import GlobalThreadPool
from sinotrans.core.rule import Rule
from email import policy
from email.parser import BytesParser
from bs4 import BeautifulSoup
from typing import Dict, Any
import concurrent.futures
import re

class EmlParser:
    """
    用于解析指定邮件文件夹的内容，根据映射字典，提取出对应的值，返回:
    {PO号：{field_name: field_value,...},...}
    """
    def __init__(self, mapping: Dict[str, Any], email_path:str):
        self.mapping = mapping
        self.email_path = email_path
    
    def extract_html_fields_value(self, html_content):
        """从邮件的HTML表格提取映射字段值，返回
        {
        des_field_nameA: des_field_valueA,
        des_field_nameB: des_field_valueB,
        ...
        }
        """
        field_values = {}
        soup = BeautifulSoup(html_content, 'html.parser')
        # 获取所有表格
        tables = soup.find_all('table')
        Logger.debug(f"📋 共找到 {len(tables)} 张表格")
        # 提取货物信息表
        # cargo_table = soup.find('table', {'class': 'MsoNormalTable'})
        for cargo_table in tables:
            for row in cargo_table.find_all('tr')[1:]:  # 跳过表头
                cells = row.find_all('td')
                # 提取每行的两个单元格内容：第一个单元格为键 (key)，第二个单元格为值 (value)。
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    # 如果mapping中存在该单元格值作为的键，获取key对应的规则列表，从中提取出des_field_name，与des_field_value结合，追加到字典中
                    rules = self.mapping.get(key)
                    if rules:
                        field_values.update(Rule.get_Map_Dict_From_List(rules, value))

            if field_values:
                return field_values
            
        return {}
    
    def decode_email_part(self, part, type):
        """解码邮件内容部分，将文本内容都记录在日志，但不作为返回值，返回的是type内容"""
        # 获取内容编码类型
        content_type = part.get_content_type()
        charset = part.get_content_charset() or 'utf-8'
        Logger.debug(f"-正在处理内容部分：{content_type}（字符集：{charset}）")

        # 将文本内容都记录在日志，但不作为返回值
        if content_type.startswith('text/plain'):
            try:
                # 获取并解码Base64内容
                payload = part.get_payload(decode=True)
                decoded_content = payload.decode(charset, errors='replace')
                # 输出解码结果
                # print("="*50)
                # print("解码后的文本内容：")
                # print(decoded_content)
                # print("="*50 + "\n")
                # return decoded_content
            except Exception as e:
                Logger.debug(f"解码失败：{str(e)}")
                # return None
        # 返回的是HTML内容
        elif content_type.startswith(type):
            body = part.get_content()
            return body
        return None
    def process_single_eml(self, filename):
        """处理单个邮件文件的线程任务,根据email_mapping返回：
        ("PO号", {
                "des_field_nameA": des_field_valueA,
                "des_field_nameB": des_field_valueB,
                ...
                }
        )
        """
        Logger.info(f"📩 处理邮件：{filename}")
        po_number = None
        try:
            # 获取邮件中的连续数字作为"PO号"
            po_match = re.search(r'(\d+)', filename)
            if not po_match:
                return (None, {})
            po_number = po_match.group(1)
            # 解析邮件文件夹下该filename的邮件内容
            eml_path = os.path.join(self.email_path, filename)
            with open(eml_path, 'rb') as f:
                msg = BytesParser(policy=policy.default).parse(f)
                # 遍历邮件内容，解析出有效内容body，body内容根据具体type形参进行设置
                for part in msg.walk():
                    body = self.decode_email_part(part, 'text/html')
                    # 如果获取到有效body内容，则立即返回二元组
                    if body:
                        return (po_number, self.extract_html_fields_value(body))
            return (po_number, {})
        except Exception as e:
            Logger.error(f"❌ 处理邮件 {filename} 失败: {str(e)}")
            return (po_number, {})
    def parse_eml_files(self, key_field: str):
        """
        解析邮件文件夹，返回结构：
        {
        "key_field_value": {邮件A映射字典},
        "key_field_value": {邮件B映射字典},
        ...
        }
        """
        global_po_mapping = {}
        files = [f for f in os.listdir(self.email_path) if f.lower().endswith('.eml')]
        Logger.info(f"📩 发现 {len(files)} 封待处理邮件")
        with GlobalThreadPool.get_executor() as executor:
            futures = [
                executor.submit(self.process_single_eml, filename)
                for filename in files
            ]
            
            for future in concurrent.futures.as_completed(futures):
                # TODO 目前默认邮件文件名中包含PO号，因此需要解析邮件文件名获取PO号——key_field_value
                key_field_value, fields = future.result()
                if key_field_value:
                    global_po_mapping[key_field_value] = fields
                    Logger.debug(f"✅ {key_field}：{key_field_value}，解析结果：{global_po_mapping[key_field_value]}")
        
        return global_po_mapping