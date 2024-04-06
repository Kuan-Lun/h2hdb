__all__ = ["__version__", "__version_info__", "H2HDB"]
__author__ = "Kuan-Lun Wang"

__version__ = "0.1.0"
__version_info__ = tuple(map(int, __version__.split(".")))


from .h2h_db import H2HDB