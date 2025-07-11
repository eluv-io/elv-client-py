import json
import shutil
import time
from pathlib import Path
from copy import copy

from requests.exceptions import HTTPError


def test_content_object_versions(client, config):
    """Test fetching content object versions"""
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    
    result = client.content_object_versions(object_id=qid, library_id=libid)

    assert 'id' in result
    assert 'versions' in result
    assert result['id'] == qid
    assert isinstance(result['versions'], list)
    assert len(result['versions']) > 0
    
    for version in result['versions']:
        assert 'id' in version
        assert 'hash' in version
        assert version['id'] == qid
        assert version['hash'].startswith('hq__')

def test_metadata_basic(client, config):
    """Test basic metadata fetching"""
    qid = config['objects']['index']['qid']
    
    result = client.content_object_metadata(object_id=qid, select='indexer/config')
    
    assert 'indexer' in result
    assert 'config' in result['indexer']
    assert 'fabric' in result['indexer']['config']
    assert 'indexer' in result['indexer']['config']

    qhash = client.content_object(object_id=qid)["hash"]

    with_hash = client.content_object_metadata(
        version_hash=qhash,
        select='indexer/config'
    )

    assert result == with_hash


def test_metadata_subtree(client, config):
    qid = config['objects']['index']['qid']
    
    result = client.content_object_metadata(
        object_id=qid, 
        metadata_subtree='indexer/config/fabric'
    )
    
    assert 'root' in result
    assert 'content' in result['root']
    assert 'library' in result['root']


def test_metadata_subtree_with_select(client, config):
    """Test metadata fetching with subtree and select"""
    qid = config['objects']['index']['qid']
    
    result = client.content_object_metadata(
        object_id=qid,
        metadata_subtree='indexer/config/fabric',
        select='root/content'
    )
    
    assert 'root' in result
    assert 'content' in result['root']
    assert 'library' not in result['root']
    assert result['root']['content'] == qid


def test_metadata_resolve_files(client, config):
    """Test metadata fetching with resolve_links=False (shouldn't matter for files)"""
    mezz = config['objects']['mezz']['12AngryMen']
    lib_mezz = config['objects']['mezz']['library']
    
    result = client.content_object_metadata(
        object_id=mezz,
        library_id=lib_mezz,
        metadata_subtree='video_tags/metadata_tags/0000/metadata_tags/celebrity_detection/tags',
        resolve_links=False
    )
    
    assert isinstance(result, list)
    assert len(result) > 0
    
    # Check structure of celebrity detection tags
    for tag in result:
        assert 'end_time' in tag
        assert 'start_time' in tag
        assert 'text' in tag
        assert isinstance(tag['text'], list)

    resolve_true = result = client.content_object_metadata(
        object_id=mezz,
        metadata_subtree='video_tags/metadata_tags/0000/metadata_tags/celebrity_detection',
        select='tags',
        resolve_links=True
    )

    assert resolve_true == result


def test_single_tag(client, config):
    """Test metadata fetching for a single tag"""
    mezz = config['objects']['mezz']['12AngryMen']
    
    result = client.content_object_metadata(
        object_id=mezz,
        metadata_subtree='video_tags/metadata_tags/0000/metadata_tags/celebrity_detection/tags/0'
    )
    
    assert 'end_time' in result
    assert 'start_time' in result
    assert 'text' in result
    assert isinstance(result['text'], list)


def test_metadata_with_remove(client, config):
    """Test metadata fetching with remove parameter"""
    mezz = config['objects']['mezz']['12AngryMen']
    lib_mezz = config['objects']['mezz']['library']
    
    result = client.content_object_metadata(
        object_id=mezz,
        library_id=lib_mezz,
        metadata_subtree='video_tags/metadata_tags/0000/metadata_tags/celebrity_detection',
        remove='tags'
    )
    
    assert 'label' in result
    assert result['label'] == 'Celebrity Detection'
    assert 'tags' not in result  # Should be removed


def test_resolve(client, config):
    """Test various resolve_links scenarios"""
    qid = config['objects']['index']['qid']
    
    # Test resolve_links=False
    result1 = client.content_object_metadata(
        object_id=qid,
        metadata_subtree='site_map/searchables/1',
        resolve_links=False
    )
    
    # Should contain a link structure
    assert '/' in result1
    assert result1['/'].startswith('/qfab/')
    
    # Test resolve_links=True
    result2 = client.content_object_metadata(
        object_id=qid,
        metadata_subtree='site_map/searchables/1',
        resolve_links=True
    )

    assert isinstance(result2, dict)

    keys = list(result2.keys())
    assert len(keys) > 0
    assert 'commit' in keys
    assert 'offerings' in keys
    assert 'transcodes' in keys


def test_search_basic(client, config):
    """Test basic search functionality"""
    qid = config['objects']['index']['qid']
    
    result = client.search(
        object_id=qid,
        query={"terms": "Lady Gaga", "limit": 1}
    )
    
    assert 'results' in result
    assert 'pagination' in result
    assert isinstance(result['results'], list)
    assert len(result['results']) <= 1  # Due to limit=1
    
    # Check pagination
    assert 'limit' in result['pagination']
    assert 'start' in result['pagination']
    assert 'total' in result['pagination']
    assert result['pagination']['limit'] == 1
    
    # Check result structure if results exist
    if result['results']:
        res = result['results'][0]
        assert 'id' in res
        assert 'hash' in res
        assert 'qlib_id' in res


def test_search_with_library_id(client, config):
    """Test search with explicit library_id"""
    qid = config['objects']['index']['qid']
    libid = config['objects']['index']['library']
    
    result = client.search(
        object_id=qid,
        library_id=libid,
        query={"terms": "Lady Gaga", "limit": 1}
    )
    
    assert 'results' in result
    assert 'pagination' in result
    assert isinstance(result['results'], list)


def test_list_files_root(client, config):
    """Test listing files at root level"""
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    
    result = client.list_files(object_id=qid, library_id=libid)
    
    assert isinstance(result, list)
    assert len(result) > 0
    
    # Should contain video file and video_tags directory
    assert 'video_tags/' in result


def test_list_files_subdirectory(client, config):
    """Test listing files in subdirectory"""
    qid = config['objects']['mezz']['12AngryMen']
    
    result = client.list_files(
        object_id=qid,
        path='video_tags'
    )
    
    assert isinstance(result, list)
    assert len(result) > 0
    
    # Should contain expected subdirectories and files
    expected_items = ['frames/', 'tracks/', 'video-tags-overlay-0000.json', 'video-tags-tracks-0000.json']
    for item in expected_items:
        assert item in result


def test_list_files_tracks(client, config):
    """Test listing files in tracks directory"""
    qid = config['objects']['mezz']['12AngryMen']
    
    result = client.list_files(
        object_id=qid,
        path='video_tags/tracks'
    )
    
    assert isinstance(result, list)
    assert len(result) > 0
    
    # Should contain JSON files
    expected_files = ['asr.json', 'caption.json', 'celeb.json', 'logo.json', 'ocr.json', 'shot.json']
    for file in expected_files:
        assert file in result


def test_list_files_with_info(client, config):
    """Test listing files with detailed info"""
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    
    result = client.list_files(
        object_id=qid,
        library_id=libid,
        path='video_tags/tracks',
        get_info=True
    )
    
    assert isinstance(result, dict)
    assert '.' in result  # Directory info
    assert result['.']['type'] == 'directory'
    
    # Check file info structure
    for filename, info in result.items():
        if filename != '.':
            assert '.' in info
            assert 'size' in info['.']
            assert 'parts' in info['.']
            assert 'encryption' in info['.']


def test_download_file(client, config):
    """Test downloading a single file"""
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    
    test_dir = Path(__file__).parent
    save_path = test_dir / 'test_downloaded.json'
    
    # Clean up any existing file
    if save_path.exists():
        save_path.unlink()
    
    try:
        client.download_file(
            object_id=qid,
            library_id=libid,
            file_path='video_tags/video-tags-tracks-0000.json',
            dest_path=str(save_path)
        )
        
        # File should exist and be valid JSON
        assert save_path.exists()
        assert save_path.stat().st_size > 0
        
        with open(save_path, 'r') as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert 'version' in data
            assert 'metadata_tags' in data
            
    finally:
        # Clean up
        if save_path.exists():
            save_path.unlink()


def test_download_directory_root(client, config):
    """Test downloading entire directory structure"""
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    
    test_dir = Path(__file__).parent
    dest_path = test_dir / 'test_downloaded_root'
    
    # Clean up any existing directory
    if dest_path.exists():
        shutil.rmtree(dest_path)
    
    try:
        client.download_directory(
            object_id=qid,
            library_id=libid,
            fabric_path='/',
            dest_path=str(dest_path)
        )
        
        # Directory should exist with expected structure
        assert dest_path.exists()
        assert dest_path.is_dir()
        
        # Should contain video_tags directory
        video_tags_dir = dest_path / 'video_tags'
        assert video_tags_dir.exists()
        assert video_tags_dir.is_dir()
        
        # Should contain the expected JSON file
        json_file = video_tags_dir / 'video-tags-tracks-0000.json'
        assert json_file.exists()
        
        with open(json_file, 'r') as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert 'version' in data
            
    finally:
        # Clean up
        if dest_path.exists():
            shutil.rmtree(dest_path)


def test_download_directory_subtree(client, config):
    """Test downloading a specific directory subtree"""
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    
    test_dir = Path(__file__).parent
    dest_path = test_dir / 'test_downloaded_subtree'
    
    # Clean up any existing directory
    if dest_path.exists():
        shutil.rmtree(dest_path)
    
    try:
        client.download_directory(
            object_id=qid,
            library_id=libid,
            fabric_path='video_tags',
            dest_path=str(dest_path)
        )
        
        # Directory should exist
        assert dest_path.exists()
        assert dest_path.is_dir()
        
        # Should contain the JSON file directly (not in video_tags subdir)
        json_file = dest_path / 'video-tags-tracks-0000.json'
        assert json_file.exists()
        
        with open(json_file, 'r') as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert 'version' in data
            
    finally:
        # Clean up
        if dest_path.exists():
            shutil.rmtree(dest_path)

def test_download_directory_subtree_slash(client, config):
    """Test downloading a specific directory subtree"""
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    
    test_dir = Path(__file__).parent
    dest_path = test_dir / 'test_downloaded_subtree'
    
    # Clean up any existing directory
    if dest_path.exists():
        shutil.rmtree(dest_path)
    
    try:
        client.download_directory(
            object_id=qid,
            library_id=libid,
            fabric_path='/video_tags/',
            dest_path=str(dest_path)
        )
        
        # Directory should exist
        assert dest_path.exists()
        assert dest_path.is_dir()
        
        # Should contain the JSON file directly (not in video_tags subdir)
        json_file = dest_path / 'video-tags-tracks-0000.json'
        assert json_file.exists()
        
        with open(json_file, 'r') as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert 'version' in data
            
    finally:
        # Clean up
        if dest_path.exists():
            shutil.rmtree(dest_path)


def test_download_files_multiple(client, config):
    """Test downloading multiple files with different paths"""
    qid = config['objects']['mezz']['12AngryMen']
    libid = config['objects']['mezz']['library']
    
    test_dir = Path(__file__).parent
    dest_path = test_dir / 'test_downloaded_multiple'
    
    # Clean up any existing directory
    if dest_path.exists():
        shutil.rmtree(dest_path)
    
    try:
        file_jobs = [
            ('video_tags/video-tags-tracks-0000.json', 'tracks.json'),
            ('video_tags/video-tags-tracks-0000.json', 'overlay.json'),
            ('video_tags/tracks/asr.json', 'asr/asr.json'),
            ('video_tags/tracks/oc.json', 'ocr/ocr.json')  # This should fail
        ]
        
        results = client.download_files(
            object_id=qid,
            library_id=libid,
            file_jobs=file_jobs,
            dest_path=str(dest_path)
        )
        
        # Should have 4 results (one for each file job)
        assert len(results) == 4
        
        # First 3 should succeed (None means success)
        assert results[0] is None
        assert results[1] is None
        assert results[2] is None
        
        # Last one should fail (file doesn't exist - should be ocr.json not oc.json)
        assert results[3] is not None
        assert isinstance(results[3], Exception)
        
        # Check that successful files were downloaded
        assert dest_path.exists()
        assert (dest_path / 'tracks.json').exists()
        assert (dest_path / 'overlay.json').exists()
        assert (dest_path / 'asr' / 'asr.json').exists()
        
    finally:
        # Clean up
        if dest_path.exists():
            shutil.rmtree(dest_path)


def test_crawl(client, crawl_token, config):
    """Test crawl status check"""
    library_id = config['objects']['index']['library']
    qid = config['objects']['index']['qid']

    latest_version = client.content_object(object_id=qid)['hash']
    
    # First start a crawl to get an LRO handle
    crawl_result = client.crawl(
        write_token=crawl_token,
        library_id=library_id,
    )

    assert isinstance(crawl_result, dict)
    assert 'lro_handle' in crawl_result 

    lro_handle = crawl_result['lro_handle']

    timeout = 15
    start = time.time()
    while True:
        assert time.time() - start < timeout, "Crawl status check timed out"
        status_result = client.crawl_status(
            write_token=crawl_token,
            lro_handle=lro_handle,
            library_id=library_id
        )

        assert isinstance(status_result, dict)
        assert 'state' in status_result

        if status_result['state'] == 'terminated':
            break

    last_crawled_hash = client.content_object_metadata(write_token=crawl_token, metadata_subtree='indexer/last_run')
    assert last_crawled_hash == latest_version, f"Expected {latest_version}, got {last_crawled_hash}"


def test_update_site(client, site_token):
    """Test site update operation"""
    site_qid = 'iq__3qRppmKKEJjrsYxgwpKtiejZuout'
    new_contents = ["iq__42WgpoYgLTyyn4MSTejY3Y4uj81o", "iq__44ExhjEWkHXtppFje9ttE2cpJcnD", "BAD_ID"]
    remove_contents = ["iq__AcgxshZahq6zM9QejDnMqs1HAjm"]

    result = client.update_site(
        site_qwt=site_token,
        ids_to_add=new_contents,
        ids_to_remove=remove_contents,
        replace_all=False
    )

    assert isinstance(result, dict)
    assert 'message' in result
    assert 'failed' in result
    assert isinstance(result['failed'], list)

    assert 'BAD_ID' in result['failed']
    
    assert "Failed to add some links" in result['message']
    
    current_site_map = client.content_object_metadata(
        write_token=site_token, 
        metadata_subtree='site_map/searchables',
        resolve_links=False
    )

    assert isinstance(current_site_map, dict)
    assert len(current_site_map) > 0

    site_qids = []
    for key, link in current_site_map.items():
        if isinstance(link, dict) and '/' in link:
            # Extract the qhash from the link and get the qid
            qhash = link['/'].split('/')[2]
            qid = client.content_object(qhash)['id']
            site_qids.append(qid)

    assert "iq__42WgpoYgLTyyn4MSTejY3Y4uj81o" in site_qids
    assert "iq__44ExhjEWkHXtppFje9ttE2cpJcnD" in site_qids

    assert "iq__AcgxshZahq6zM9QejDnMqs1HAjm" not in site_qids

    assert "BAD_ID" not in site_qids

def test_http_error_with_invalid_object_id(client):
    """Test that HTTPError messages include JSON error body for invalid object ID"""
    
    error = None
    try:
        client.content_object_metadata(object_id="iq__BAD")
    except HTTPError as err:
        error = err

    assert error is not None

    assert error.response.status_code == 404

    err_msg = json.loads(error.response.text)

    print(err_msg)

    assert isinstance(err_msg, dict)
    assert 'errors' in err_msg


def test_http_error_with_invalid_metadata(client, config):
    """Test that HTTPError messages include JSON error body for invalid object ID"""

    qid = config['objects']['mezz']['12AngryMen']
    
    error = None
    try:
        client.content_object_metadata(object_id=qid, metadata_subtree='does/not/exist')
    except HTTPError as err:
        error = err

    assert error is not None

    assert error.response.status_code == 404

    err_msg = json.loads(error.response.text)

    print(json.loads(error.response.text))

    assert isinstance(err_msg, dict)
    assert 'errors' in err_msg

def test_http_error_with_bad_auth(client, config):
    """Test that HTTPError messages include JSON error body for invalid object ID"""

    client = copy(client)

    client.set_static_token('BAD')

    qid = config['objects']['mezz']['12AngryMen']
    
    error = None
    try:
        client.content_object_metadata(object_id=qid, metadata_subtree='does/not/exist')
    except HTTPError as err:
        error = err

    assert error is not None

    # 400 because unknown auth scheme
    assert error.response.status_code == 400

    err_msg = json.loads(error.response.text)

    print(json.loads(error.response.text))

    assert isinstance(err_msg, dict)
    assert 'errors' in err_msg