import os
import argparse
from typing import Any, List, Callable
from loguru import logger

from src.elv_client import *
from quick_test_py import Tester

# contains tests that depend on write token

config = {
    'fabric_url': 'https://host-154-14-185-100.contentfabric.io/config?self&qspace=main',
    'qid': 'iq__42WgpoYgLTyyn4MSTejY3Y4uj81o',
    'libid': 'ilib4JvLVStm2pDMa89332h8tNqUCZvY',
    'env_auth_token': 'TEST_AUTH_TOKEN',
    'env_write_token': 'WRITE_TOKEN'
}

def test_merge_metadata(client: ElvClient) -> List[Callable]:
    qwt = os.getenv(config['env_write_token'])
    if not qwt:
        raise Exception(f"Please set {config['env_write_token']} environment variable")

    def t1():
        metadata = client.content_object_metadata(write_token=qwt, resolve_links=False)
        metadata['test'] = {"hello": "world", "foo": {"bar": 1}}
        client.merge_metadata(write_token=qwt, library_id=config["libid"], metadata=metadata)
        r1 = client.content_object_metadata(write_token=qwt, metadata_subtree="test")
        r2 = client.content_object_metadata(write_token=qwt, metadata_subtree="test/foo/bar")
        client.merge_metadata(write_token=qwt, library_id=config["libid"], metadata=2, metadata_subtree="test/foo/bar")
        r3 = client.content_object_metadata(write_token=qwt, metadata_subtree="test/foo/bar")
        return [r1, r2, r3]
    
    return [t1]
    
def test_set_metadata(client: ElvClient) -> List[Callable]:
    qwt = os.getenv(config['env_write_token'])
    if not qwt:
        raise Exception(f"Please set {config['env_write_token']} environment variable")

    def t1():
        metadata = client.content_object_metadata(write_token=qwt, resolve_links=False)
        metadata['test'] = {"hello": "world", "foo": {"bar": 1}}
        client.replace_metadata(write_token=qwt, library_id=config["libid"], metadata=metadata)
        r1 = client.content_object_metadata(write_token=qwt, metadata_subtree="test/hello")
        r2 = client.content_object_metadata(write_token=qwt, metadata_subtree="test/foo/bar")
        metadata['test']['foo']['bar'] = 2
        client.replace_metadata(write_token=qwt,library_id=config["libid"], metadata=metadata)
        r3 = client.content_object_metadata(write_token=qwt, metadata_subtree="test/foo/bar")
        return [r1, r2, r3]
    
    return [t1]

def test_upload_files(client: ElvClient) -> List[Callable]:
    qwt = os.getenv(config['env_write_token'])
    if not qwt:
        raise Exception(f"Please set {config['env_write_token']} environment variable")
    filedir = os.path.dirname(os.path.abspath(__file__))
    def t1():
        jobs = [ElvClient.FileJob(local_path=os.path.join(filedir, 'test.txt'), out_path='dir1/test.txt', mime_type='text/plain'), \
                ElvClient.FileJob(local_path=os.path.join(filedir, 'test.json'), out_path='dir2/test.json', mime_type='application/json')]
        client.upload_files(write_token=qwt, library_id=config['libid'], file_jobs=jobs)
        return client.list_files(write_token=qwt, library_id=config['libid'])

    return [t1]

def main():
    cwd = os.path.dirname(os.path.abspath(__file__))
    tester = Tester(os.path.join(cwd, 'test_data'))
    TOK = os.getenv(config['env_auth_token'])   
    client = ElvClient.from_configuration_url(config['fabric_url'], static_token=TOK)
    tester.register('merge_metadata_test', test_cases=test_merge_metadata(client))
    tester.register('set_metadata_test', test_cases=test_set_metadata(client))
    tester.register('upload_files_test', test_cases=test_upload_files(client))
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