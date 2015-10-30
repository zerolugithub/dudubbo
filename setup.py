import os
import sys

from setuptools import find_packages, setup
from dubbo.dubbo import __version__

setup(
    name = 'PyDubbo',
    version = __version__,
    description = 'python dubbo rpc framework client',
    keywords = 'dubbo hessian2 java',
    url = 'https://github.com/dmall/dudubbo',
    author = 'zhouyou, ly0',
    author_email = 'zhouyoug@gmail.com, latyas@gmail.com',
    packages = find_packages(exclude = ['temp.*', '*.class', '*.jar'])
)
