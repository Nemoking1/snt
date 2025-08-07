from sinotrans.utils import Logger
from typing import Optional, Dict, Any
import threading
import concurrent.futures

class GlobalThreadPool:
    """
    全局线程池管理器
    """
    # 线程池实例
    _executor: Optional[concurrent.futures.ThreadPoolExecutor] = None # 延迟初始化
    # 锁对象，用于确保线程安全
    _lock = threading.Lock()
    # 线程池配置参数
    _config: Dict[str, Any] = {
        'max_workers': None,
        'thread_name_prefix': '',
        'initializer': None, # 初始化函数方法
        'initargs': () # 初始化函数的参数
    }

    @classmethod
    def initialize(cls, **kwargs) -> None:
        """初始化全局线程池（线程安全）"""
        Logger.debug(f"尝试获取锁以初始化线程池，当前线程: {threading.current_thread().name}")
        try:
            with cls._lock:
                if cls._executor is not None and not cls._executor._shutdown:
                    cls._executor.shutdown(wait=True)
                
                # 更新配置参数
                valid_keys = {'max_workers', 'thread_name_prefix', 'initializer', 'initargs'}
                cls._config.update((k, v) for k, v in kwargs.items() if k in valid_keys)
                
                cls._executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=cls._config['max_workers'],
                    thread_name_prefix=cls._config['thread_name_prefix'],
                    initializer=cls._config['initializer'],
                    initargs=cls._config['initargs']
                )
        finally:
            Logger.debug(f"释放锁，当前线程: {threading.current_thread().name}")

    @classmethod
    def get_executor(cls) -> concurrent.futures.ThreadPoolExecutor:
        """获取线程池实例，若没有则创建实例，延迟加载"""
        if cls._executor is None or cls._executor._shutdown:
            cls.initialize()
        return cls._executor

    @classmethod
    def shutdown(cls, wait: bool = True) -> None: # 在声明类方法时没有写 cls 参数，Python 解释器会抛出异常
        """关闭线程池并释放资源"""
        with cls._lock:
            if cls._executor and not cls._executor._shutdown:
                cls._executor.shutdown(wait=wait)
                cls._executor = None