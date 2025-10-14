import argparse
import os
from typing import Any, Callable, List

from loguru import logger
from quick_test_py import Tester

from src.elv_client import *

config = {
    'fabric_url': 'http://192.168.96.203',
    'config_url': 'http://192.168.96.203/config?self&qspace=main',
    'env_auth_token': 'TEST_AUTH',
    'objects': {
        "mezz": {"library": "ilib4JvLVStm2pDMa89332h8tNqUCZvY",
                 "qid": "iq__42WgpoYgLTyyn4MSTejY3Y4uj81o"},
    }
}
TOK = os.getenv(config['env_auth_token'])
client = ElvClient.from_configuration_url(
    config["config_url"], static_token=TOK)


def test_download_part() -> List[Callable]:
    qid = config['objects']['mezz']['qid']
    libid = config['objects']['mezz']['library']
    video_part = client.content_object_metadata(
        version_hash=qid, metadata_subtree='offerings/default/media_struct/streams/video/sources/0')['source']
    audio_part = client.content_object_metadata(
        version_hash=qid, metadata_subtree='offerings/default/media_struct/streams/audio/sources/0')['source']
    logger.debug(f"Video part hash: {video_part}")
    logger.debug(f"Audio part hash: {audio_part}")
    filedir = os.path.dirname(os.path.abspath(__file__))
    if os.path.exists(os.path.join(filedir, 'out.mp4')):
        os.remove(os.path.join(filedir, 'out.mp4'))
    if os.path.exists(os.path.join(filedir, 'out.aac')):
        os.remove(os.path.join(filedir, 'out.aac'))

    def t1():
        save_path = os.path.join(filedir, 'out.mp4')
        print(f"Downloading video part to {save_path}")
        client.download_part(object_id=qid, library_id=libid,
                             part_hash=video_part, save_path=save_path)
        return os.path.exists(save_path) and os.path.getsize(save_path) > 1e6

    def t2():
        save_path = os.path.join(filedir, 'out.aac')
        print(f"Saving audio part to {save_path}")
        client.download_part(object_id=qid, library_id=libid,
                             part_hash=audio_part, save_path=save_path)
        return os.path.exists(save_path) and os.path.getsize(save_path) > 1e5
    return [t1, t2]


def test_download_unencrypted_part() -> List[Callable]:
    qid = config['objects']['mezz']['qid']
    libid = config['objects']['mezz']['library']
    part = "hqp_6cfwb5cd1rmuaQ7Niqm3JKHnXSgwTHdAmJfzjzTdLz1ARNc"
    filedir = os.path.dirname(os.path.abspath(__file__))
    if os.path.exists(os.path.join(filedir, 'out.txt')):
        os.remove(os.path.join(filedir, 'out.txt'))

    def t1():
        save_path = os.path.join(filedir, 'out.txt')
        print(f"Downloading part to {save_path}")
        client.download_part(
            object_id=qid, part_hash=part, save_path=save_path)
        assert os.path.exists(save_path) and os.path.getsize(save_path) > 1e3
        return "passed"
    return [t1]


def main():
    cwd = os.path.dirname(os.path.abspath(__file__))
    tester = Tester(os.path.join(cwd, 'test_results'))
    for test in [test_download_part, test_download_unencrypted_part]:
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
