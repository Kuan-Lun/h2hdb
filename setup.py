from setuptools import setup
import re
import os


def read_version():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(script_dir, "hentaidb", "__init__.py")) as f:
        return re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read(), re.M).group(1)


setup(
    name="hentaidb",
    version=read_version(),
    author="Kuan-Lun Wang",
    author_email="kuan-lun@klwang.tw",
    description="A simple Hentai at Home database",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=["hentaidb"],
    install_requires=['mysql-connector-python'],
)

# Path: hentaidb/__init__.py
