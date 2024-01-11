import unittest
import logging
from src.elv_client import *

logging.basicConfig(level=logging.INFO)

TOK = ''
CONFIG = 'https://main.net955305.contentfabric.io/config'

class ElvClientTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(TOK != '', 'Auth token not set')
        self.assertTrue(CONFIG != '', 'Config URL not set')
        self.client = ElvClient.from_configuration_url("https://main.net955305.contentfabric.io/config", static_token=TOK)   

    def test_metadata(self):
        self.assertTrue(TOK != '', 'Auth token not set')
        meta = self.client.content_object_metadata(object_id='iq__44VReNyWedZ1hAACRDBF6TdrBXAE', metadata_subtree='/indexer/config/fabric', select=['root'])
        logging.info(meta)

    def test_search(self):
        self.assertTrue(TOK != '', 'Auth token not set')
        res = self.client.search(object_id='iq__2oENKiVcWj9PLnKjYupCw1wduUxj', query={"terms":"hello", "search_fields": ["f_speech_to_text"], "limit": 1})
        logging.info(res)

if __name__ == '__main__':
    unittest.main()