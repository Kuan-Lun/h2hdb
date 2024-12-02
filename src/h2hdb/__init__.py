__all__ = [
    "__version__",
    "__version_info__",
    "H2HDB",
    "DatabaseConfig",
    "LoggerConfig",
    "H2HConfig",
    "Config",
    "load_config",
    "HentaiDBLogger",
    "setup_logger",
]
__author__ = "Kuan-Lun Wang"

__version__ = "0.6.68.54"
__version_info__ = tuple(map(int, __version__.split(".")))


from .h2h_db import H2HDB
from .config_loader import DatabaseConfig, LoggerConfig, H2HConfig, Config, load_config
from .logger import HentaiDBLogger, setup_logger
