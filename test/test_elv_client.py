import os
import argparse
from typing import Any, List, Callable
from loguru import logger

from src.elv_client import *
from quick_test_py import Tester

config = {
    'fabric_config': 'https://main.net955305.contentfabric.io/config',
    'env_auth_token': 'TEST_AUTH_TOKEN',
    'objects': {
        "mezz": {"library":"ilib4JvLVStm2pDMa89332h8tNqUCZvY", 
                 "12AngryMen": "iq__b7ZBuXBYAqiwCc5oirFZEdfWY6v"},
        "index": {"library":"ilib2hqtVe6Ngwa7gM4uLMFzjJapJsTd", "qid": "iq__3qRppmKKEJjrsYxgwpKtiejZuout"},
    }
}

def test_versions(client: ElvClient) -> List[Callable]:
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    t1 = lambda: client.content_object_versions(object_id=qid, library_id=libid)
    return [t1]

def test_metadata(client: ElvClient) -> List[Callable]:
    qid = config['objects']['index']['qid']
    mezz = config['objects']['mezz']['12AngryMen']
    lib_mezz = config['objects']['mezz']['library']
    t1 = lambda: client.content_object_metadata(object_id=qid, select='indexer/config')
    t2 = lambda: client.content_object_metadata(object_id=qid, metadata_subtree='indexer/config/fabric')
    t3 = lambda: client.content_object_metadata(object_id=qid, metadata_subtree='indexer/config/fabric', select='root/content')
    test_resolve = lambda: client.content_object_metadata(object_id=mezz, library_id=lib_mezz,
                                                           metadata_subtree='video_tags/metadata_tags/0000/metadata_tags/celebrity_detection/tags', resolve_links=True)
    t5 = lambda: client.content_object_metadata(object_id=mezz, metadata_subtree='video_tags/metadata_tags/0000/metadata_tags/celebrity_detection', select='tags', resolve_links=True)
    t6 = lambda: client.content_object_metadata(object_id=mezz, 
                                                           metadata_subtree='video_tags/metadata_tags/0000/metadata_tags/celebrity_detection/tags/0', resolve_links=True)
    t7 = lambda: client.content_object_metadata(object_id=mezz, library_id=lib_mezz,
                                                           metadata_subtree='video_tags/metadata_tags/0000/metadata_tags/celebrity_detection', remove='tags', resolve_links=True)
    return [t1, t2, t3, test_resolve, t5, t6, t7]

def test_search(client: ElvClient) -> List[Callable]:
    qid = config['objects']['index']['qid']
    libid = config['objects']['index']['library']
    def postprocess(out: dict) -> dict:
        for res in out["results"]:
            del res['score']
        return out
    t1 = lambda: postprocess(client.search(object_id=qid, query={"terms":"Lady Gaga", "limit": 1}))
    t2 = lambda: postprocess(client.search(object_id=qid, library_id=libid, query={"terms":"Lady Gaga", "limit": 1, "offset": 1}))
    return [t1, t2]


def main():
    cwd = os.path.dirname(os.path.abspath(__file__))
    tester = Tester(os.path.join(cwd, 'test_data'))
    TOK = os.getenv(config['env_auth_token'])   
    client = ElvClient.from_configuration_url(config['fabric_config'], static_token=TOK)
    tester.register('versions_test', test_cases=test_versions(client))
    tester.register('metadata_test', test_cases=test_metadata(client))
    tester.register('search_test', test_cases=test_search(client))
    if args.record:
        tester.record(args.tests)
    else:
        tester.validate(args.tests)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--record', action='store_true', help='Save outputs to ground truth')
    parser.add_argument('--tests', nargs='+', default=None, help='Run specific tests')
    args = parser.parse_args()
    main()