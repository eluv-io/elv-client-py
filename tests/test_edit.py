import pytest
import os
import json
from datetime import datetime
from pathlib import Path

from src.elv_client import ElvClient

def test_merge_metadata_basic(client, crawl_token, config):
    """Test merging metadata at root and subtree levels"""
    qlib = config['objects']['index']['library']
    
    # Get current metadata
    original_metadata = client.content_object_metadata(
        write_token=crawl_token,
        resolve_links=False
    )
    
    # Add test metadata
    updated_metadata = dict(original_metadata)
    updated_metadata['test'] = {"hello": "world", "foo": {"bar": 1}}
    
    client.merge_metadata(
        write_token=crawl_token,
        library_id=qlib,
        metadata=updated_metadata
    )
    
    # Verify the merged metadata
    result1 = client.content_object_metadata(
        write_token=crawl_token,
        metadata_subtree="test"
    )
    
    assert result1 == {"hello": "world", "foo": {"bar": 1}}
    assert result1['hello'] == "world"
    assert result1['foo']['bar'] == 1
    
    # Verify nested value
    result2 = client.content_object_metadata(
        write_token=crawl_token,
        metadata_subtree="test/foo/bar"
    )
    
    assert result2 == 1
    
    # Merge metadata at subtree level
    client.merge_metadata(
        write_token=crawl_token,
        library_id=qlib,
        metadata=2,
        metadata_subtree="test/foo/bar"
    )
    
    # Verify the updated value
    result3 = client.content_object_metadata(
        write_token=crawl_token,
        metadata_subtree="test/foo/bar"
    )
    
    assert result3 == 2


def test_commit_message(client, crawl_token, config):
    """Test setting commit message and verifying commit timestamp"""
    qlib = config['objects']['index']['library']
    
    # Get the object ID from the write token
    object_id = client.content_object(write_token=crawl_token)["id"]
    
    # Get the old commit timestamp
    old_timestamp = client.content_object_metadata(
        object_id=object_id,
        metadata_subtree="commit/timestamp"
    )
    
    # Set a new commit message
    client.set_commit_message(
        write_token=crawl_token,
        library_id=qlib,
        message="test commit"
    )
    
    # Verify the commit message was set
    commit_message = client.content_object_metadata(
        write_token=crawl_token,
        metadata_subtree="commit/message"
    )
    
    assert commit_message == "test commit"
    
    # Get the new commit timestamp
    commit_time = client.content_object_metadata(
        write_token=crawl_token,
        metadata_subtree="commit/timestamp"
    )
    
    old_dt = datetime.fromisoformat(old_timestamp.replace("Z", "+00:00"))
    new_dt = datetime.fromisoformat(commit_time.replace("Z", "+00:00"))
    
    assert new_dt > old_dt


def test_replace_metadata(client, crawl_token, config):
    """Test replacing metadata at root and subtree levels"""
    qlib = config['objects']['index']['library']
    
    # Get current metadata
    original_metadata = client.content_object_metadata(
        write_token=crawl_token,
        resolve_links=False
    )
    
    # Add test metadata
    updated_metadata = dict(original_metadata)
    updated_metadata['test'] = {"hello": "world", "foo": {"bar": 1}}
    
    client.replace_metadata(
        write_token=crawl_token,
        library_id=qlib,
        metadata=updated_metadata
    )
    
    # Verify the replaced metadata
    result1 = client.content_object_metadata(
        write_token=crawl_token,
        metadata_subtree="test/hello"
    )
    
    assert result1 == "world"
    
    result2 = client.content_object_metadata(
        write_token=crawl_token,
        metadata_subtree="test/foo/bar"
    )
    
    assert result2 == 1
    
    # Update the value
    updated_metadata['test']['foo']['bar'] = 2
    
    client.replace_metadata(
        write_token=crawl_token,
        library_id=qlib,
        metadata=updated_metadata
    )
    
    # Verify the updated value
    result3 = client.content_object_metadata(
        write_token=crawl_token,
        metadata_subtree="test/foo/bar"
    )
    
    assert result3 == 2


def test_upload_and_download_files(client, crawl_token, config, temp_dir):
    """Test uploading files and then downloading them back"""
    qlib = config['objects']['index']['library']
    
    # Get the test data directory
    test_dir = Path(__file__).parent.parent / 'test' / 'test_data' / 'caption'
    
    # Skip if test data doesn't exist
    if not test_dir.exists():
        pytest.skip("Test data directory not found")
    
    # Create file jobs for upload
    jobs = []
    for path in sorted(os.listdir(test_dir)):
        if path.endswith('.json'):
            jobs.append(ElvClient.FileJob(
                local_path=str(test_dir / path),
                out_path=f'video_tags/caption/{path}',
                mime_type='application/json'
            ))
    
    # Skip if no files to upload
    if not jobs:
        pytest.skip("No caption files found to upload")
    
    # Upload the files
    client.upload_files(
        write_token=crawl_token,
        library_id=qlib,
        file_jobs=jobs
    )
    
    # Verify files were uploaded by listing them
    file_list = client.list_files(
        write_token=crawl_token,
        library_id=qlib,
        path='video_tags/caption'
    )
    
    assert isinstance(file_list, list)
    assert len(file_list) > 0
    
    # Download the files back
    download_dir = Path(temp_dir) / 'downloaded'
    download_dir.mkdir(parents=True, exist_ok=True)
    
    downloaded_data = []
    for job in jobs:
        filename = os.path.basename(job.local_path)
        dest_path = download_dir / f'downloaded_{filename}'
        
        client.download_file(
            write_token=crawl_token,
            library_id=qlib,
            file_path=job.out_path,
            dest_path=str(dest_path)
        )
        
        # Verify the file exists and is valid JSON
        assert dest_path.exists()
        
        with open(dest_path, 'r') as f:
            data = json.load(f)
            downloaded_data.append(data)
            assert isinstance(data, (dict, list))
    
    # Verify we downloaded the expected number of files
    assert len(downloaded_data) == len(jobs)
    
    # Verify downloaded content matches original
    for i, job in enumerate(jobs):
        with open(job.local_path, 'r') as f:
            original_data = json.load(f)
            assert downloaded_data[i] == original_data