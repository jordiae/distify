import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="distify",
    version="0.8.5",
    description="Easy wrapper for parallelizing Python executions",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/jordiae/distify",
    author="Jordi Armengol-Estapé",
    author_email="me@jordiae.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.0",

    ],
    packages=["distify"],
    include_package_data=True,
    install_requires=[
        'ray',
        'hydra-core',
        'hydra-ray-launcher',
        'tqdm',
        'aiohttp>=3.7',
        'sqlitedict',
        'ordered-set'
    ]

)
