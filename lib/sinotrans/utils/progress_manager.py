import threading
from tqdm import tqdm 

class ProgressManager:
    _instance = None
    
    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.lock = threading.Lock()
            cls._instance.main_pbar = None
        return cls._instance
    
    def init_main_progress(self, total, desc="description", unit="行", min_interval=1, max_interval=5, bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} , {rate_fmt}"):
        """初始化主进度条（只能调用一次）"""
        if self.main_pbar is None:
            self.main_pbar = tqdm(
                total=total,
                desc=desc,
                unit=unit,
                mininterval=min_interval,  # 最小刷新间隔，过小会影响效率，过大会导致进度条没来得及更新
                maxinterval=max_interval,   # 最大刷新间隔
                bar_format=bar_format
            )
    
    def update(self):
        with self.lock:
            if self.main_pbar:
                self.main_pbar.update(1)
    
    def close(self):
        if self.main_pbar:
            # self.main_pbar.disable = True 
            self.main_pbar.close()


class ExcelProgressTracker:
    """跟踪Excel处理进度的工具类,非单例模式"""
    
    def __init__(self):
        self.main_pbar = None
    
    def init_main_progress(self, total, desc="处理进度", unit="行", min_interval=1, max_interval=5, 
                          bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} , {rate_fmt}"):
        """初始化进度条
        
        Args:
            total: 总进度单位数
            desc: 进度条描述
            unit: 进度单位
            min_interval: 最小刷新间隔(秒)
            max_interval: 最大刷新间隔(秒)
            bar_format: 进度条格式
        """
        if self.main_pbar is None:
            self.main_pbar = tqdm(
                total=total,
                desc=desc,
                unit=unit,
                mininterval=min_interval,
                maxinterval=max_interval,
                bar_format=bar_format
            )
    
    def update(self):
        """更新进度条"""
        if self.main_pbar:
            self.main_pbar.update(1)
    
    def close(self):
        """关闭进度条"""
        if self.main_pbar:
            self.main_pbar.close()
            self.main_pbar = None  # 重置状态，允许再次初始化