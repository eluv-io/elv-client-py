import os

from elv_client_py.elv_client import ElvClient

def test_live_download(live_download_client: ElvClient, config, temp_dir):
    qid = config['objects']['livestream']['qid']
    output_path = f'{temp_dir}/out.mp4'
    info = live_download_client.live_media_segment(qid, output_path, segment_idx=0, segment_length=4)
    assert os.path.exists(output_path)
    assert os.path.getsize(output_path) > 1e6
    assert info is not None

    with open(output_path, 'rb') as f:
        data_1 = f.read()

    # check that downloading the same segment again gives the same data
    output_path_dup = f'{temp_dir}/out_dup.mp4'
    info_dup = live_download_client.live_media_segment(qid, output_path_dup, segment_idx=0, segment_length=4)
    assert os.path.exists(output_path_dup)
    assert os.path.getsize(output_path_dup) > 1e6
    assert info_dup is not None
    with open(output_path_dup, 'rb') as f:
        data_dup = f.read()
    assert data_1 == data_dup

    # check that the next segment downloads and is different than the first
    output_path_2 = f'{temp_dir}/out2.mp4'
    info_2 = live_download_client.live_media_segment(qid, output_path_2, segment_idx=1, segment_length=4)
    assert os.path.exists(output_path_2)
    assert os.path.getsize(output_path_2) > 1e6
    assert info_2 is not None
    assert info_2.seg_offset_millis > 0
    # check raw data is different

    with open(output_path_2, 'rb') as f:
        data_2 = f.read()

    assert data_1 != data_2