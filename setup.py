from setuptools import setup

setup(
    name = "elv-client-py",
    version='0.1',
    packages=['elv_client_py'],
    install_requires=['requests', 
                      'base58',
                      'loguru',
                      'quick_test_py @ git+https://github.com/eluv-io/quick-test-py.git#egg=quick_test_py'],
)