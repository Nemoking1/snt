from sinotrans.utils.logger import Logger
from sinotrans.utils.global_thread_pool import GlobalThreadPool
from sinotrans.core.rule import Rule
from email import policy
from email.parser import BytesParser
from bs4 import BeautifulSoup
from typing import Dict, Any, List
import concurrent.futures
import imaplib
import random
import time
import re
import os

class EmlParser:
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
                Logger.debug(f"❌ 解码失败：{str(e)}")
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
    
class EmailClient:
    """用于对邮箱进行操作"""

    mail = None
    def __init__(self, imap_server, imap_port, imap_username, imap_password, selected_box="INBOX", max_retries=5):
        self.imap_server = imap_server
        self.imap_port = imap_port
        self.imap_username = imap_username
        self.imap_password = imap_password
        self.selected_box = selected_box  # 默认邮箱
        self.max_retries = max_retries  # 最大重试次数
    def noop(self, max_retries=3):
        """发送NOOP心跳命令，保持连接活跃，不受装饰器修饰（因为装饰器中也使用了NOOP命令），重试时选择重置连接
        
        使用指数退避策略进行重试，包含随机抖动以避免同步重试
        自动处理连接重置和重连
        
        Raises:
            RuntimeError: 当超过最大重试次数仍失败时抛出
        """
        Logger.debug("💓 发送NOOP心跳保持连接")
        for attempt in range(1, self.max_retries + 1):
            try:
                # 前置状态检查
                if not self.mail or self.mail.state not in ['SELECTED', 'AUTH']:
                    Logger.info("🔁 IMAP连接已断开，正在重新连接...")
                    self.connect_imap(self.selected_box)
                
                # 执行NOOP命令
                response = self.mail.noop()
                if not response or response[0] != 'OK':
                    raise RuntimeError(f"⚠️ NOOP响应异常: {response}")
                
                Logger.debug("✅ NOOP成功")
                
            except Exception as e:
                Logger.error(f"❌ NOOP失败 (尝试 {attempt}/{self.max_retries}): {str(e)}")
                if attempt < max_retries:
                    delay = min(60, 2 ** attempt + random.uniform(0, 1))  # 指数退避+随机抖动
                    Logger.info(f"等待 {delay:.2f} 秒后重试...")
                    time.sleep(delay)
                    
                    self._reset_connection()
                else:
                    raise RuntimeError(
                        f"{self.imap_username}@{self.imap_server}:{self.imap_port} "
                        f"❌ NOOP失败，超过最大重试次数: {str(e)}"
                    ) from e
                

    def _reset_connection(self):
        """安全地登出IMAP连接，self.mail = None"""
        try:
            if self.mail:
                try:
                    self.mail.logout()
                except Exception as logout_error:
                    Logger.error(f"❌ 登出时发生错误: {str(logout_error)}")
        except Exception as e:
            Logger.error(f"❌ 重置连接时发生错误: {str(e)}")
        finally:
            self.mail = None
    def _retry_imap_operation(self, operation, *args, **kwargs):
        """重置连接，重试IMAP操作的装饰器"""
        for attempt in range(1, self.max_retries + 1):
            try:
                # 确保连接有效
                if not self.mail or self.mail.state != 'SELECTED':
                    Logger.info("🔁 IMAP连接已断开，正在重新连接...")
                    self.connect_imap(self.selected_box)
                    self.noop()
                    
                return operation(*args, **kwargs)
            except Exception as e:
                Logger.error(f"⚠️ IMAP状态错误 (尝试 {attempt}/{self.max_retries}): {e}")
                if attempt < self.max_retries:
                    # 添加：指数退避策略
                    delay = min(60, 2 ** attempt)  # 指数退避，最大60秒
                    Logger.info(f"等待 {delay} 秒后重试...")
                    time.sleep(delay + random.uniform(0, 1))  # 添加随机抖动
                    
                    self._reset_connection()
                else:
                    raise RuntimeError(f"{self.imap_username}:{self.imap_password}无法连接到服务器: {self.imap_server}:{self.imap_port}") from e
        return None
    def connect_imap(self, selected_box, max_retries=3):
        """
        根据配置连接IMAP服务器（SSL）并登录，默认重试3次，如果失败则抛出异常
        返回：IMAP对象
        """
        if not selected_box: 
            selected_box = self.selected_box

        for attempt in range(1, max_retries + 1):
            try:
                mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
                mail.login(self.imap_username, self.imap_password)
                # 选择收件箱，可改为其他文件夹如 'Spam'
                mail.select(selected_box)
                self.mail = mail
                return mail
            except Exception as e:
                Logger.debug(f"⚠️ 连接失败 (尝试 {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    # 添加：指数退避策略
                    delay = min(60, 2 ** attempt)  # 指数退避，最大60秒
                    Logger.debug(f"等待 {delay} 秒后重试...")
                    time.sleep(delay + random.uniform(0, 1))  # 添加随机抖动
                else:
                    raise RuntimeError(f"❌ 重试失败：{e}")
    def search_mail(self, condition, keyword):
        """
        搜索邮件（带重试机制）
        :param condition: 搜索条件
        :param keyword: 关键字，如"ALL"
        :return: 邮件列表
        """
        def _search():
            status, messages = self.mail.uid('SEARCH', condition, keyword)
            return status, messages
        return self._retry_imap_operation(_search)
    def fetch_email_by_uid(self, email_uid, keyword):
        """
        获取指定 UID 的邮件内容——原始邮件数据（带重试机制）

        Args:
            email_uid (bytes or str): 邮件唯一标识符

        Returns:
            tuple: (status, msg_data) 原始邮件数据
        """
        def _fetch():
            status, msg_data = self.mail.uid('FETCH', email_uid, keyword)
            return status, msg_data
        return self._retry_imap_operation(_fetch)
    def copy_email_by_uid(self, email_uid, utf7_folder):
        """
        将指定 UID 的邮件复制到目标文件夹（带重试机制）

        Args:
            email_uid (bytes or str): 邮件唯一标识符
            utf7_folder (str): 目标文件夹名（UTF-7 编码格式）

        Returns:
            str: IMAP 操作结果状态
        """
        def _copy():
            copy_result = self.mail.uid('COPY', email_uid, utf7_folder)
            return copy_result
        return self._retry_imap_operation(_copy)
    def delete_email_by_uids(self, email_uids: List[str]):
        """删除邮件（带重试机制）并验证是否成功"""
        def _delete():
            for email_uid in email_uids:
                # 标记删除
                delete_result = self.mail.uid('STORE', email_uid, '+FLAGS', '\\Deleted')
                Logger.debug(f'🛑 删除邮件 {email_uid} 结果：{delete_result}')
            # 提交删除操作
            self.mail.expunge()

        try:
            self._retry_imap_operation(_delete)
            success_flags = []
            for email_uid in email_uids:
                # 检查邮件是否存在（可能需要延迟）
                time.sleep(3)  # 等待服务器处理
                status, data = self.fetch_email_by_uid(email_uid, '(RFC822)')
                if status == 'OK' and data == [None]:
                    Logger.info(f"✅ 邮件 {email_uid} 删除成功")
                    success_flags.append(True)
                else:
                    Logger.info(f"❌ 邮件 {email_uid} 删除失败（可能已不存在）")
                    success_flags.append(False)
            return all(success_flags)
        except Exception as e:
            Logger.error(f"❌ 删除错误: {e}")
            return False

    def check_exist_mailbox(self, folder_name):
        """
        检查邮箱中是否存在指定的文件夹
        
        Args:
            mail: 已认证的IMAP邮箱连接对象
            folder_name: 要检查的文件夹名称（需符合IMAP命名规范）
        
        Returns:
            bool: True表示文件夹存在，False表示文件夹不存在
        """
        if not folder_name or not isinstance(folder_name, str):
            raise RuntimeError("🚨 文件夹名称不能为空且必须是字符串")
        def _check():
            # 获取邮箱现有文件夹列表
            status, folders = self.mail.list()
            if status != "OK" or not folders or len(folders) <= 0:
                raise RuntimeError(f"❌ 获取有效邮箱列表失败：{status}")
            
            exists = False
            for folder_info in folders:
                try:
                    # 兼容不同IMAP服务器的响应格式（如：'(\\HasNoChildren) "/" "INBOX"'）
                    decoded_info = folder_info.decode('utf-8', errors='ignore')
                    parts = [p.strip() for p in decoded_info.split('"/"')]
                    
                    if len(parts) >= 2:
                        # 提取标准化文件夹名（去除引号和空格）
                        existing_name = parts[-1].strip('"\' ')
                        if existing_name.casefold() == folder_name.casefold():
                            exists = True
                            break
                except Exception as e:
                    raise RuntimeError (f"❌ 解析文件夹信息异常：{e}")
            return exists
        return self._retry_imap_operation(_check)
    def create_mailbox(self, folder_name):
        """
        在邮箱中创建文件夹（邮箱目录）
        输入:
        mail: 已认证的IMAP邮箱连接对象
        folder_name: 要创建的文件夹名称（需符合IMAP命名规范）

        输出:
        None

        可能抛出的异常：
        RuntimeError: 如果文件夹创建失败
        """
        if not folder_name or not isinstance(folder_name, str):
            raise RuntimeError("🚨 文件夹名称不能为空且必须是字符串")
        def _create():
            # 创建新文件夹
            typ, response = self.mail.create(folder_name)
            if typ == 'OK':
                Logger.info(f"📁 文件夹创建成功：'{folder_name}'")
                # 1. 短暂延迟让服务器同步
                time.sleep(1)  # 300ms延迟，根据网络状况调整
                # 2. 发送NOOP命令刷新连接状态
                self.mail.noop()
                # 3. 验证文件夹是否真实存在
                if self.check_exist_mailbox(folder_name):
                    Logger.debug(f"✅ 验证成功：'{folder_name}' 已存在")
                else:
                    raise RuntimeError(f"❌ 文件夹创建失败（{typ}）：{response}")
            else:
                raise RuntimeError(f"❌ 文件夹创建失败（{typ}）：{response}")
        return self._retry_imap_operation(_create)
                
    def copy_eml_to_folder(self, email_uid, folder_name):
        """
        先判断文件夹存不存在，然后移动邮件到指定文件夹
        """
        def _copy():
            # 1.先判断文件夹存不存在
            if not self.check_exist_mailbox(folder_name):
                raise RuntimeError(f"❌ 文件夹不存在：{folder_name}")
            
            # 2. 将字符串形式的文件夹名（如 "收件箱"）编码为 UTF-7 格式，这是 IMAP 协议要求的格式。然后将字节类型的 UTF-7 字符串解码为 Python 的字符串类型，以便后续用于 IMAP 命令中
            s_utf7 = folder_name.encode('utf-7').replace(b'+', b'&').replace(b',', b'-')
            utf7_folder = s_utf7.decode('ascii')

            # 3. 执行复制操作
            copy_result = self.copy_email_by_uid(email_uid, utf7_folder)
            if copy_result[0] != 'OK':
                raise RuntimeError(f"❌ 邮件复制失败：{email_uid} -> {utf7_folder}，错误：{copy_result[1]}")
            else:
                Logger.info(f"✉️ 原邮件复制到：{utf7_folder}")
                
        return self._retry_imap_operation(_copy)
        #copy_result = mail.uid('COPY', email_uid, utf7_folder)