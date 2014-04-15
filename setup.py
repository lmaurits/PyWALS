#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from pywals import __version__ as version

setup(
    name='PyWALS',
    version=version,
    description='Pythonic interface to World Atlas of Language Structures',
    author='Luke Maurits',
    author_email='luke@maurits.id.au',
    url='https://github.com/lmaurits/pywals',
    license="BSD (3 clause)",
    classifiers=[
        'Programming Language :: Python',
        'License :: OSI Approved :: BSD License',
    ],
    packages = ['pywals',],

)
