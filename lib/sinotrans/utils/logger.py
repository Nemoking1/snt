import logging
import os
from datetime import datetime
from typing import Optional

class Logger:
    """
    日志记录器
    """
    _instance: Optional['Logger'] = None
    _initialized = False
    
    def __new__(cls, timeFormat="%Y%m%d_%H%M%S", desc = "appName", debug_path: str = "logs", log_level: int = logging.DEBUG):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._setup(timeFormat, desc, debug_path, log_level)
        return cls._instance
    
    def _setup(self, timeFormat, desc,  debug_path: str, log_level: int):
        if self._initialized:
            return
        
        self.timestamp = datetime.now().strftime(timeFormat)
        self.DEBUG_PATH = debug_path
        
        # 确保日志目录存在
        os.makedirs(self.DEBUG_PATH, exist_ok=True)
        
        # 基础配置
        self.logger = logging.getLogger(desc)
        self.logger.setLevel(log_level)
        
        # 文件处理器
        file_handler = logging.FileHandler(
            filename=os.path.join(self.DEBUG_PATH, f"debug_{self.timestamp}.log"),
            encoding="utf-8"
        )
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter("%(message)s")
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)  # 控制台只显示INFO及以上级别
        
        # 添加处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self._initialized = True
    
    @staticmethod
    def info(message: str):
        Logger().logger.info(message)
    
    @staticmethod
    def debug(message: str):
        Logger().logger.debug(message)
    
    @staticmethod
    def error(message: str, exc_info: bool = False):
        Logger().logger.error(message, exc_info=exc_info)
    
    @staticmethod
    def exception(message: str):
        """记录异常信息（自动包含堆栈跟踪）"""
        Logger().logger.error(message, exc_info=True)
