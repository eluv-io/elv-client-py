import os
import pytest
from src.elv_client import ElvClient


@pytest.fixture
def client():
    """
    Fixture that provides a read-only ElvClient instance.
    Requires TEST_AUTH environment variable to be set.
    """
    auth_token = os.getenv('TEST_AUTH')
    if not auth_token:
        pytest.skip("TEST_AUTH environment variable not set")
    
    fabric_config = 'https://main.net955305.contentfabric.io/config'
    return ElvClient.from_configuration_url(fabric_config, auth_token)


@pytest.fixture
def site_token():
    """
    Fixture that provides a site write token.
    Requires SITE_WRITE_TOKEN environment variable to be set.
    """
    site_write_token = os.getenv('SITE_WRITE_TOKEN')
    if not site_write_token:
        pytest.skip("SITE_WRITE_TOKEN environment variable not set")
    
    return site_write_token


@pytest.fixture
def crawl_token():
    """
    Fixture that provides a crawl write token.
    Requires CRAWL_WRITE_TOKEN environment variable to be set.
    """
    crawl_write_token = os.getenv('CRAWL_WRITE_TOKEN')
    if not crawl_write_token:
        pytest.skip("CRAWL_WRITE_TOKEN environment variable not set")
    
    return crawl_write_token


@pytest.fixture
def config():
    """
    Fixture that provides test configuration data.
    """
    return {
        'fabric_config': 'https://main.net955305.contentfabric.io/config',
        'env_auth_token': 'TEST_AUTH',
        'objects': {
            "mezz": {"library": "ilib4JvLVStm2pDMa89332h8tNqUCZvY",
                     "12AngryMen": "iq__b7ZBuXBYAqiwCc5oirFZEdfWY6v"},
            "index": {"library": "ilib2hqtVe6Ngwa7gM4uLMFzjJapJsTd", "qid": "iq__3qRppmKKEJjrsYxgwpKtiejZuout"},
        }
    }

