from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path


here = path.abspath(path.dirname(__file__))


# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='onesource',

    version='1.0.0',

    description='Text processing pipeline',

    long_description=long_description,

    url='',

    author='Mark Moloney',

    author_email='',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Data Pipelines',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6'
    ],

    keywords='NLP pipeline',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    install_requires=['lxml', 'mock', 'pytest', 'PyYAML', 'ray', 'spacy'],

    entry_points={
        'console_scripts': [
            'onesource=onesource:main'
        ]
    }
)
