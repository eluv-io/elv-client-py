import os

from elv_client_py.elv_client import ElvClient

def test_live_download(live_download_client: ElvClient, config, temp_dir):
    qid = config['objects']['livestream']['qid']
    output_path = f'{temp_dir}/out.mp4'
    live_download_client.live_media_segment(qid, output_path)
    assert os.path.exists(output_path)
    assert os.path.getsize(output_path) > 1e6