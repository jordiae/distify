import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="distify",
    version="0.1.9",
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
        'aiohttp==3.7.4.post0',
        'aiohttp-cors==0.7.0',
        'aioredis==1.3.1',
        'antlr4-python3-runtime==4.8',
        'async-timeout==3.0.1',
        'attrs==21.2.0',
        'blessings==1.7',
        'boto3==1.15.6',
        'botocore==1.18.18',
        'cachetools==4.2.2',
        'certifi==2021.5.30',
        'chardet==4.0.0',
        'charset-normalizer==2.0.5',
        'click==8.0.1',
        'cloudpickle==2.0.0',
        'colorful==0.5.4',
        'filelock==3.0.12',
        'gitdb==4.0.7',
        'GitPython==3.1.18',
        'google-api-core==1.31.2',
        'google-auth==1.35.0',
        'googleapis-common-protos==1.53.0',
        'gpustat==0.6.0',
        'grpcio==1.40.0',
        'hiredis==2.0.0',
        'hydra-core==1.1.1',
        'hydra-ray-launcher==0.1.2',
        'idna==3.2',
        'jmespath==0.10.0',
        'jsonschema==3.2.0',
        'msgpack==1.0.2',
        'multidict==5.1.0',
        'numpy==1.21.2',
        'nvidia-ml-py3==7.352.0',
        'omegaconf==2.1.1',
        'opencensus==0.7.13',
        'opencensus-context==0.1.2',
        'packaging==21.0',
        'pickle5==0.0.11',
        'prometheus-client==0.11.0',
        'protobuf==3.17.3',
        'psutil==5.8.0',
        'py-spy==0.3.9',
        'pyasn1==0.4.8',
        'pyasn1-modules==0.2.8',
        'pyparsing==2.4.7',
        'pyrsistent==0.18.0',
        'python-dateutil==2.8.2',
        'pytz==2021.1',
        'PyYAML==5.4.1',
        'ray==1.6.0',
        'redis==3.5.3',
        'requests==2.26.0',
        'rsa==4.7.2',
        's3transfer==0.3.7',
        'six==1.16.0',
        'smmap==4.0.0',
        'tqdm==4.62.2',
        'typing-extensions==3.10.0.2',
        'urllib3==1.25.11',
        'yarl==1.6.3'
    ]

)
