
import argparse
import json
import os
from datetime import datetime
from typing import Callable, List

from quick_test_py import Tester

from src.elv_client import *

# contains tests that depend on write token

config = {
    'fabric_url': 'https://host-154-14-185-100.contentfabric.io/config?self&qspace=main',
    'qid': 'iq__42WgpoYgLTyyn4MSTejY3Y4uj81o',
    'libid': 'ilib4JvLVStm2pDMa89332h8tNqUCZvY',
    'env_auth_token': 'TEST_AUTH',
    'env_write_token': 'WRITE_TOKEN'
}

TOK = os.getenv(config['env_auth_token'])
client = ElvClient.from_configuration_url(
    config['fabric_url'], static_token=TOK)


def test_merge_metadata() -> List[Callable]:
    qwt = os.getenv(config['env_write_token'])
    if not qwt:
        raise Exception(
            f"Please set {config['env_write_token']} environment variable")

    def t1():
        metadata = client.content_object_metadata(
            write_token=qwt, resolve_links=False)
        metadata['test'] = {"hello": "world", "foo": {"bar": 1}}
        client.merge_metadata(
            write_token=qwt, library_id=config["libid"], metadata=metadata)
        r1 = client.content_object_metadata(
            write_token=qwt, metadata_subtree="test")
        r2 = client.content_object_metadata(
            write_token=qwt, metadata_subtree="test/foo/bar")
        client.merge_metadata(
            write_token=qwt, library_id=config["libid"], metadata=2, metadata_subtree="test/foo/bar")
        r3 = client.content_object_metadata(
            write_token=qwt, metadata_subtree="test/foo/bar")
        return [r1, r2, r3]

    def test_commit():
        client.set_commit_message(
            write_token=qwt, library_id=config["libid"], message="test commit")
        commit_message = client.content_object_metadata(
            write_token=qwt, metadata_subtree="commit/message")
        assert commit_message == "test commit"
        object_id = client.content_object(write_token=qwt)["id"]
        old_timestamp = client.content_object_metadata(
            object_id=object_id, metadata_subtree="commit/timestamp")
        commit_time = client.content_object_metadata(
            write_token=qwt, metadata_subtree="commit/timestamp")
        # load strings as datetime and make sure new commit time is greater than old commit time
        assert datetime.fromisoformat(commit_time.replace(
            "Z", "+00:00")) > datetime.fromisoformat(old_timestamp.replace("Z", "+00:00"))
        return ["Passed"]

    return [t1, test_commit]


def test_set_metadata() -> List[Callable]:
    qwt = os.getenv(config['env_write_token'])
    if not qwt:
        raise Exception(
            f"Please set {config['env_write_token']} environment variable")

    def t1():
        metadata = client.content_object_metadata(
            write_token=qwt, resolve_links=False)
        metadata['test'] = {"hello": "world", "foo": {"bar": 1}}
        client.replace_metadata(
            write_token=qwt, library_id=config["libid"], metadata=metadata)
        r1 = client.content_object_metadata(
            write_token=qwt, metadata_subtree="test/hello")
        r2 = client.content_object_metadata(
            write_token=qwt, metadata_subtree="test/foo/bar")
        metadata['test']['foo']['bar'] = 2
        client.replace_metadata(
            write_token=qwt, library_id=config["libid"], metadata=metadata)
        r3 = client.content_object_metadata(
            write_token=qwt, metadata_subtree="test/foo/bar")
        return [r1, r2, r3]

    return [t1]


def test_upload_files() -> List[Callable]:
    qwt = os.getenv(config['env_write_token'])
    if not qwt:
        raise Exception(
            f"Please set {config['env_write_token']} environment variable")
    filedir = os.path.dirname(os.path.abspath(__file__))

    def t1():
        jobs = []
        for path in sorted(os.listdir(os.path.join(filedir, 'test_data', 'caption'))):
            jobs.append({
                'local_path': os.path.join(filedir, 'test_data', 'caption', path),
                'out_path': f'video_tags/caption/{path}',
                'mime_type': 'application/json'
            })
        jobs = [ElvClient.FileJob(**job) for job in jobs]
        client.upload_files(
            write_token=qwt, library_id=config['libid'], file_jobs=jobs)
        res1 = client.list_files(write_token=qwt, library_id=config['libid'])
        res2 = []
        os.makedirs(os.path.join(filedir, 'downloaded'), exist_ok=True)
        for job in jobs:
            dest_path = os.path.join(
                filedir, 'downloaded', f'downloaded_{os.path.basename(job.local_path)}')
            client.download_file(
                write_token=qwt, library_id=config['libid'], file_path=job.out_path, dest_path=dest_path)
            with open(dest_path, 'r') as f:
                res2.append(json.load(f))
        return [res1, res2]

    return [t1]


def main():
    cwd = os.path.dirname(os.path.abspath(__file__))
    tester = Tester(os.path.join(cwd, 'test_results'))
    for test in [test_merge_metadata, test_set_metadata, test_upload_files]:
        tester.register(test)

    if args.record:
        tester.record(args.tests)
    else:
        tester.validate(args.tests)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--record', action='store_true',
                        help='Save outputs to ground truth')
    parser.add_argument('--tests', nargs='+', default=None,
                        help='Run specific tests')
    args = parser.parse_args()
    main()
