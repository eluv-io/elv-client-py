from setuptools import setup

setup(
    name = "elv-client-py",
    version='0.1',
    packages=['elv_client_py'],
    install_requires=['requests==2.31.0', 'base58==2.1.1'],
)