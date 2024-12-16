# elv-client-py
Python SDK for the Eluvio Content Fabric 

## Install package via pip
`pip install git+https://github.com/eluv-io/elv-client-py.git`

## Local testing 

All tests are against musicgreen tenancy

1. `pip install .`
2. Set auth token for test tenant `export TEST_AUTH=...` (should have transaction for test_edit.py and test_part_download.py)
3. Set auth token for edit access `export WRITE_TOKEN=...` (only for test_edit.py) 
2.  `python test/test_elv_client.py && python test/test_edit.py && test/test_part_download.py`