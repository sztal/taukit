#!/usr/bin/env python

import os
import sys

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup


if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

readme = open('README.rst').read()
doclink = """
Documentation
-------------

The full documentation is at http://taukit.rtfd.org."""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='taukit',
    version='0.0.0',
    description='Python toolkit for developing simple and moderately complex data collection, processing and management projects.',
    long_description=readme + '\n\n' + doclink + '\n\n' + history,
    author='Szymon Talaga',
    author_email='stalaga@protonmail.com',
    url='https://github.com/sztal/taukit',
    packages=[*find_packages()],
    setup_requires=[
        'pytest-runner>=4.2,<5.0'
    ],
    tests_require=[
        'pytest>=4.2.0,<5.0.0',
        'pylint>=2.1.1,<3.0.0',
        'pytest-pylint>=0.12.2,<1.0.0',
        'pytest-doctestplus>=0.2.0,<1.0.0',
        'coverage>=4.5.1,<5.0.0'
        'pytest-cov>=2.6.1,<3.0.0'
    ],
    test_suite='tests',
    package_dir={'taukit': 'taukit'},
    include_package_data=True,
    install_requires=[
        'cerberus>=1.2,<2.0',
        'scrapy>=1.6.0,<2.0.0',
        'w3lib>=1.20.0,<2.0.0',
        'tldextract>=2.2.0,<3.0.0',
        'click>=7.0,<8.0',
        'dateparser>=0.7.0,<1.0.0',
        'typing>=3.6.6,<4.0.0',
        'ipython>=7.4.0,<8.0.0',
        'mongoengine>=0.17.0,<1.0.0',
        'pymongo>=3.7.2,<4.0.0',
        'joblib>=0.13.2,<1.0.0'
    ],
    license='MIT',
    zip_safe=False,
    keywords='taukit',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
