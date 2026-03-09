"""
测试配置和环境变量管理
"""

import os
import sys
import platform
from pathlib import Path

# 获取项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent


def get_default_driver_path():
    """根据操作系统返回默认驱动路径"""
    if platform.system() == "Windows":
        return str(PROJECT_ROOT / "build_win64" / "bin" / "maxcompute_odbc.dll")
    else:
        return str(PROJECT_ROOT / "build" / "lib" / "libmaxcompute_odbc.dylib")


class TestConfig:
    """测试配置类"""
    
    # 默认配置
    DEFAULT_DRIVER_PATH = get_default_driver_path()
    DEFAULT_ENDPOINT = "https://service.cn-shanghai.maxcompute.aliyun.com/api"
    DEFAULT_PROJECT = ""
    DEFAULT_ACCESS_KEY_ID = ""
    DEFAULT_ACCESS_KEY_SECRET = ""
    DEFAULT_QUOTA_NAME = ""
    
    # 从环境变量加载配置
    @classmethod
    def load_from_env(cls):
        """从环境变量加载配置"""
        return cls(
            driver_path=os.getenv("MCO_DRIVER_PATH", cls.DEFAULT_DRIVER_PATH),
            endpoint=os.getenv("MAXCOMPUTE_ENDPOINT", cls.DEFAULT_ENDPOINT),
            project=os.getenv("MAXCOMPUTE_PROJECT", cls.DEFAULT_PROJECT),
            access_key_id=os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID", cls.DEFAULT_ACCESS_KEY_ID),
            access_key_secret=os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", cls.DEFAULT_ACCESS_KEY_SECRET),
            quota_name=os.getenv("MAXCOMPUTE_QUOTA_NAME", cls.DEFAULT_QUOTA_NAME),
        )
    
    def __init__(
        self,
        driver_path: str = None,
        endpoint: str = None,
        project: str = None,
        access_key_id: str = None,
        access_key_secret: str = None,
        quota_name: str = None,
    ):
        self.driver_path = driver_path or self.DEFAULT_DRIVER_PATH
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.project = project or self.DEFAULT_PROJECT
        self.access_key_id = access_key_id or self.DEFAULT_ACCESS_KEY_ID
        self.access_key_secret = access_key_secret or self.DEFAULT_ACCESS_KEY_SECRET
        self.quota_name = quota_name or self.DEFAULT_QUOTA_NAME
    
    def validate(self) -> tuple[bool, str]:
        """验证配置是否有效"""
        if not self.project:
            return False, "Project is not configured"
        if not self.access_key_id:
            return False, "Access Key ID is not configured"
        if not self.access_key_secret:
            return False, "Access Key Secret is not configured"
        if not os.path.exists(self.driver_path):
            return False, f"Driver file not found: {self.driver_path}"
        return True, "Configuration is valid"
    
    def create_connection_string(
        self,
        interactive_mode: bool = False,
        quota_name: str = None,
        timeout: int = None,
    ) -> str:
        """创建连接字符串"""
        conn_str = (
            f"DRIVER={{MaxCompute ODBC Driver}};"
            f"Endpoint={self.endpoint};"
            f"Project={self.project};"
            f"AccessKeyId={self.access_key_id};"
            f"AccessKeySecret={self.access_key_secret};"
        )
        
        if interactive_mode:
            conn_str += "interactiveMode=true;"
        else:
            conn_str += "interactiveMode=false;"
        
        if quota_name:
            conn_str += f"quotaName={quota_name};"
        elif self.quota_name:
            conn_str += f"quotaName={self.quota_name};"
        
        if timeout:
            conn_str += f"LoginTimeout={timeout};"
        
        return conn_str
    
    def __repr__(self):
        return (
            f"TestConfig(driver_path={self.driver_path}, "
            f"endpoint={self.endpoint}, "
            f"project={self.project}, "
            f"quota_name={self.quota_name})"
        )


# 全局配置实例
config = TestConfig.load_from_env()


def print_config():
    """打印当前配置（隐藏敏感信息）"""
    print("=" * 60)
    print("Test Configuration")
    print("=" * 60)
    print(f"Driver Path: {config.driver_path}")
    print(f"Endpoint: {config.endpoint}")
    print(f"Project: {config.project}")
    print(f"Access Key ID: {config.access_key_id[:10]}...{config.access_key_id[-4:] if config.access_key_id else 'None'}")
    print(f"Access Key Secret: {'*' * 8 if config.access_key_secret else 'None'}")
    print(f"Quota Name: {config.quota_name or 'None'}")
    print("=" * 60)


if __name__ == "__main__":
    print_config()
    is_valid, message = config.validate()
    if is_valid:
        print(f"✓ {message}")
    else:
        print(f"✗ {message}")
        sys.exit(1)