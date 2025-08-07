# import os
# import re
# from email import policy
# from email.parser import BytesParser
# from bs4 import BeautifulSoup
# from typing import Dict, Any
# from ..utils.logging import logger

# class EmlParser:
#     def __init__(self, mapping: Dict[str, Any]):
#         self.mapping = mapping
    
#     def parse_eml(self, eml_path: str) -> Dict[str, Any]:
#         """解析单个eml文件"""
#         try:
#             with open(eml_path, 'rb') as f:
#                 msg = BytesParser(policy=policy.default).parse(f)
#                 return self._extract_fields(msg)
#         except Exception as e:
#             logger.error(f"Failed to parse {eml_path}: {str(e)}")
#             return {}

#     def _extract_fields(self, msg) -> Dict[str, Any]:
#         """从邮件内容提取字段"""
#         fields = {}
#         for part in msg.walk():
#             if part.get_content_type() == 'text/html':
#                 soup = BeautifulSoup(part.get_content(), 'html.parser')
#                 for table in soup.find_all('table'):
#                     for row in table.find_all('tr')[1:]:
#                         cells = row.find_all('td')
#                         if len(cells) >= 2:
#                             key = cells[0].get_text(strip=True)
#                             value = cells[1].get_text(strip=True)
#                             if key in self.mapping:
#                                 fields.update(self._map_field(key, value))
#         return fields

#     def _map_field(self, key: str, value: str) -> Dict[str, Any]:
#         """应用字段映射规则"""
#         return {rule['field_name']: value for rule in self.mapping.get(key, [])}