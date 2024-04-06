from setuptools import setup
import re
import os


def read_version():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(script_dir, "src", "h2hdb", "__init__.py")) as f:
        return re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read(), re.M).group(1)


setup(
    name="h2hdb",
    version=read_version(),
    author="Kuan-Lun Wang",
    author_email="kuan-lun@klwang.tw",
    description="A simple H@H database",
    license="GNU Affero General Public License",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    keywords=["H@H", "h2h", "database", "h2hdb"],
    packages=["h2hdb"],
    install_requires=[],
    extras_require={"MySQL": ["mysql-connector-python>=8.3.0,<9.0.0"]},
    package_dir={"": "src"},
    python_requires=">=3.12, <4",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
    ],
)
