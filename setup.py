from setuptools import setup

setup(
    name = "elv-client-py",
    version='0.1',
    packages=['elv_client_py'],
    include_package_data=True,
    package_data={'elv_client_py': ['config.yml']},
    install_requires=['requests', 
                      'aiohttp',
                      'base58',
                      'loguru',
                      'pyyaml',
                      'tqdm',
                      'quick_test_py @ git+https://github.com/eluv-io/quick-test-py.git#egg=quick_test_py'],
)