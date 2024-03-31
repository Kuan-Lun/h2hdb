import unittest
import logging
from io import StringIO


from hentaidb import setup_logger


class TestLogger(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = setup_logger("DEBUG")

    def setUp(self) -> None:
        self.log_output = StringIO()
        self.handler = logging.StreamHandler(self.log_output)
        self.logger.addHandler(self.handler)

    def test_debug(self) -> None:
        self.logger.debug("這是一條 debug 級別的日誌")
        log_output = self.log_output.getvalue().strip()
        self.assertIn("這是一條 debug 級別的日誌", log_output)

    def test_info(self) -> None:
        self.logger.info("這是一條 info 級別的日誌")
        log_output = self.log_output.getvalue().strip()
        self.assertIn("這是一條 info 級別的日誌", log_output)

    def test_warning(self) -> None:
        self.logger.warning("這是一條 warning 級別的日誌")
        log_output = self.log_output.getvalue().strip()
        self.assertIn("這是一條 warning 級別的日誌", log_output)

    def test_error(self) -> None:
        self.logger.error("這是一條 error 級別的日誌")
        log_output = self.log_output.getvalue().strip()
        self.assertIn("這是一條 error 級別的日誌", log_output)

    def test_critical(self) -> None:
        self.logger.critical("這是一條 critical 級別的日誌")
        log_output = self.log_output.getvalue().strip()
        self.assertIn("這是一條 critical 級別的日誌", log_output)
