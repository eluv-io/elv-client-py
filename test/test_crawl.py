from argparse import ArgumentParser
import os
from typing import List, Callable
import time

from quick_test_py import Tester

from src.elv_client import ElvClient

config = {
    'fabric_config': 'https://main.net955305.contentfabric.io/config',
    'env_auth_token': 'TEST_AUTH',
    'crawl_qwt': 'TEST_QWT',
    'site_qwt': 'TEST_QWT2',
}

def crawl_test() -> List[Callable]:
    qwt = os.getenv(config['crawl_qwt'])
    auth = os.getenv(config['env_auth_token'])

    client = ElvClient.from_configuration_url(config['fabric_config'], static_token=auth)

    def t1():
        qid = client.content_object(write_token=qwt)['id']
        latest_version = client.content_object(object_id=qid)['hash']
        lro_status = client.crawl(write_token=qwt)
        lro_handle = lro_status['lro_handle']
        while client.crawl_status(write_token=qwt, lro_handle=lro_handle)['state'] != 'terminated':
            time.sleep(5)
        last_crawled_hash = client.content_object_metadata(write_token=qwt, metadata_subtree='indexer/last_run')
        assert last_crawled_hash == latest_version, f"Expected {latest_version}, got {last_crawled_hash}"
        return ["passed"]

    return [t1]

def site_test() -> List[Callable]:
    qwt = os.getenv(config['site_qwt'])
    auth = os.getenv(config['env_auth_token'])
    assert qwt, "Site QWT not set in environment variables"
    assert auth, "Auth token not set in environment variables"

    site_qid = 'iq__3qRppmKKEJjrsYxgwpKtiejZuout'
    new_content = ["iq__42WgpoYgLTyyn4MSTejY3Y4uj81o", "iq__44ExhjEWkHXtppFje9ttE2cpJcnD", "BAD_ID"]
    remove_contents = ["iq__AcgxshZahq6zM9QejDnMqs1HAjm"]

    client = ElvClient.from_configuration_url(config['fabric_config'], static_token=auth)

    def t1():
        qid = client.content_object(write_token=qwt)['id']
        assert qid == site_qid, "Write token should reference test site QID"

        status = client.update_site(site_qwt=qwt, ids_to_add=new_content, ids_to_remove=remove_contents)

        site_meta = client.content_object_metadata(write_token=qwt, metadata_subtree='site_map/searchables')

        for link in site_meta.values():
            del link["."]

        return [status, site_meta]

    return [t1]

def main():
    filedir = os.path.dirname(os.path.abspath(__file__))
    tester = Tester(os.path.join(filedir, 'test_data'))

    all_tests = [crawl_test, site_test]

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
