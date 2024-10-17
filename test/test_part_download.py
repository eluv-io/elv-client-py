import os
import argparse
from typing import Any, List, Callable
from loguru import logger

from src.elv_client import *
from quick_test_py import Tester

config = {
    'fabric_url': 'https://host-76-74-28-233.contentfabric.io',
    'env_auth_token': 'PART_TOKEN',
    'objects': {
        "mezz": {"library":"ilib2HWBxwsXrgtRzgMVVxAzm1oPH53U", 
                 "caminandes": "iq__4R5mtJVCKDL6tLAqgHyPqDNm6RDL"},
    }
}

def test_download_part(client: ElvClient) -> List[Callable]:
    qid = config['objects']['mezz']['caminandes']
    libid = config['objects']['mezz']['library']
    video_part = client.content_object_metadata(version_hash=qid, metadata_subtree='/offerings/default/media_struct/streams/video/sources/0')['source']
    audio_part = client.content_object_metadata(version_hash=qid, metadata_subtree='/offerings/default/media_struct/streams/audio/sources/0')['source']
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
        client.download_part(object_id=qid, library_id=libid, part_hash=video_part, save_path=save_path)
        return os.path.exists(save_path) and os.path.getsize(save_path) > 1e7
    def t2():
        save_path=os.path.join(filedir, 'out.aac')
        print(f"Saving audio part to {save_path}")
        client.download_part(object_id=qid, library_id=libid, part_hash=audio_part, save_path=save_path)
        return os.path.exists(save_path) and os.path.getsize(save_path) > 5e5
    return [t1, t2]

def main():
    cwd = os.path.dirname(os.path.abspath(__file__))
    tester = Tester(os.path.join(cwd, 'test_data'))
    TOK = os.getenv(config['env_auth_token'])   
    client = ElvClient([config['fabric_url']], static_token=TOK)
    client2 = ElvClient.from_configuration_url('https://host-76-74-28-233.contentfabric.io/config?self&qspace=demov3', static_token=TOK)
    tester.register('download_part_test', test_cases=test_download_part(client))
    tester.register('download_part_test_from_config', test_cases=test_download_part(client2))
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