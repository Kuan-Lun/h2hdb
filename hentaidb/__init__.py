__all__ = ["__version__", "__version_info__", "ComicDB", "parse_gallery_info"]
__author__ = "Kuan-Lun Wang"

__version__ = "0.0.3"
__version_info__ = tuple(map(int, __version__.split(".")))


from .gallery_info_parser import parse_gallery_info
from .sql_connector import ComicDB
