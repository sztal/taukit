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
    packages=['taukit'],
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest',
        'pylint',
        'pytest-pylint',
        'pytest-benchmark',
        'pytest-doctestplus',
        'coverage'
        'pytest-cov'
    ],
    test_suite='tests',
    package_dir={'taukit': 'taukit'},
    include_package_data=True,
    install_requires=[
        'cerberus>=1.2',
        'scrapy>=1.6.0',
        'tldextract>=2.2.0',
        'click>=7.0',
        'dateparser>=0.7.0'
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
