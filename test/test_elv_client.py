import unittest
import os
import argparse
import json
from src.elv_client import *

TOK = os.getenv('TEST_AUTH_TOKEN')
# TODO: tests should point to some demo tenant instead
CONFIG = 'https://main.net955305.contentfabric.io/config'

save_state = False

class ElvClientTest(unittest.TestCase):
    def setUp(self):
        if TOK is None or CONFIG is None:
            raise EnvironmentError('Auth token or Config URL not set')
        if len(os.listdir('test_data')) == 0:
            raise FileNotFoundError('No test data found, please run `python test_elv_client.py --save_state`')
        self.client = ElvClient.from_configuration_url(CONFIG, static_token=TOK)

    def test_versions(self):
        res = []
        res.append(self.client.content_object_versions(object_id='iq__44VReNyWedZ1hAACRDBF6TdrBXAE', library_id='ilib31RD8PXrsdvSppy2p78LU3C9JdME'))
        save_file = 'versions.json'
        if save_state:
            record(res, save_file)
        else:
            self.validate(res, save_file)

    def test_metadata(self):
        res = []
        res.append(self.client.content_object_metadata(object_id='iq__44VReNyWedZ1hAACRDBF6TdrBXAE', metadata_subtree='indexer/config/fabric', select='root'))
        res.append(self.client.content_object_metadata(object_id='iq__44VReNyWedZ1hAACRDBF6TdrBXAE', select='indexer/config'))
        res.append(self.client.content_object_metadata(object_id='iq__44VReNyWedZ1hAACRDBF6TdrBXAE', metadata_subtree='indexer/config/fabric'))
        save_file = 'metadata.json'
        if save_state:
            record(res, save_file)
        else:
            self.validate(res, save_file)

    def test_search(self):
        res = []
        res.append(self.client.search(object_id='iq__2oENKiVcWj9PLnKjYupCw1wduUxj', query={"terms":"hello", "search_fields": ["f_speech_to_text"], "limit": 1}))
        save_file = 'search.json'
        if save_state:
            record(res, save_file)
        else:
            self.validate(res, save_file)

    def validate(self, out: List[Any], name: str) -> None:
        with open(os.path.join('test_data', name), 'r') as fin:
            data = json.load(fin)
        for out, ground_truth in zip(out, data):
            self.assertEqual(out, ground_truth)

def record(out: List[Any], name: str) -> None:
    with open(os.path.join('test_data', name), 'w') as fout:
        json.dump(out, fout, indent=4)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--save_state', action='store_true', default=False, help='Save outputs to ground truth')

    args, unknown = parser.parse_known_args()
    
    save_state = args.save_state
    unittest.main(argv=[__file__] + unknown)