__all__ = ["__version__", "__version_info__", "TestLogger"]

__author__ = "Kuan-Lun Wang"

__version__ = "0.0.1"
__version_info__ = tuple(map(int, __version__.split(".")))

from .test_logger import TestLogger