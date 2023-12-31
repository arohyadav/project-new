from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='mypackage',
    version='0.1',
    packages=find_packages(include=['data_processing.*']),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'mydataprocessing = main:main',
        ],
    },
)
