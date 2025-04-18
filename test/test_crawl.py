from argparse import ArgumentParser
import os
from typing import List, Callable
import time

from quick_test_py import Tester

from src.elv_client import ElvClient

config = {
    'fabric_config': 'https://main.net955305.contentfabric.io/config',
    'search_uris': ["https://host-154-14-185-100.contentfabric.io"],
    'env_auth_token': 'TEST_AUTH',
    'env_write_token': 'TEST_QWT',
}

def crawl_test() -> List[Callable]:
    qwt = os.getenv('TEST_QWT')
    auth = os.getenv('TEST_AUTH')

    client = ElvClient.from_configuration_url(config['fabric_config'], static_token=auth)
    client.search_uris = config['search_uris']

    def t1():
        qid = client.content_object(write_token=qwt)['id']
        latest_version = client.content_object(object_id=qid)['hash']
        lro_status = client.crawl(write_token=qwt)
        while client.crawl_status(write_token=qwt, lro_handle=lro_status)['state'] != 'terminated':
            time.sleep(5)
        last_crawled_hash = client.content_object_metadata(write_token=qwt, metadata_subtree='indexer/last_run')
        assert last_crawled_hash == latest_version, f"Expected {latest_version}, got {last_crawled_hash}"
        return ["passed"]

    return [t1]

def main():
    filedir = os.path.dirname(os.path.abspath(__file__))
    tester = Tester(os.path.join(filedir, 'test_data'))

    all_tests = [crawl_test]
    
    for test in all_tests:
        tester.register(test)

    if args.record:
        tester.record(args.tests)
    else:
        tester.validate(args.tests)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--record', action='store_true')
    parser.add_argument('--tests', nargs='+', type=str, default=None, help='Tests to run')
    args = parser.parse_args()
    main()
