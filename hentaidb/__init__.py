__all__ = ["__version__", "__version_info__", "hentaiDB"]
__author__ = "Kuan-Lun Wang"

__version__ = "0.0.3"
__version_info__ = tuple(map(int, __version__.split(".")))


from .sql_connector import hentaiDB
