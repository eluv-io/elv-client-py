
from typing import Any, Dict
from typing import List, Optional
from loguru import logger
import requests
from dataclasses import dataclass
import os

from .utils import get, build_url, post

class ElvClient():
    def __init__(self, fabric_uris: List[str], search_uris: List[str]=[], static_token: str=""):
        self.fabric_uris = fabric_uris
        self.search_uris = search_uris
        self.token = static_token

    @staticmethod
    def from_configuration_url(config_url: str, static_token: str=""):
        config = get(config_url)
        services = config.get("network", {}).get("services", {})
        if len(services) == 0:
            raise Exception("No services available in the configuration")
        if "fabric_api" not in services:
            raise Exception("No Fabric URIs available in the configuration")
        fabric_uris = services["fabric_api"]
        if not fabric_uris:
            raise Exception("No Fabric URIs available in the configuration")
        search_uris = services.get("search_v2", [])
        if not search_uris:
            logger.warning("No Search URIs available in the configuration")
        return ElvClient(fabric_uris, search_uris, static_token)
    
    def set_static_token(self, token: str):
        self.token = token

    def _get_host(self) -> str:
        if len(self.fabric_uris) == 0:
            raise Exception("No Fabric URIs available")
        return self.fabric_uris[0]
    
    def _get_search_host(self) -> str:
        if len(self.search_uris) == 0:
            raise Exception("No Search URIs available")
        return self.search_uris[0]
    
    def content_object_metadata(self, 
                                library_id: Optional[str]=None, 
                                object_id: Optional[str]=None, 
                                version_hash: Optional[str]=None, 
                                write_token: Optional[str]=None,
                                metadata_subtree: str="",
                                select: Optional[str]=None, 
                                remove: Optional[str]=None,
                                resolve_links: bool=False
                                ) -> Any:
        if not self.token:
            raise Exception("No token available")
        url = self._get_host()
        id = write_token or version_hash or object_id
        if not id:
            raise Exception("Object ID, Version Hash, or Write Token must be specified")
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        url = build_url(url, 'q', id, 'meta', metadata_subtree)
        headers = {"Authorization": f"Bearer {self.token}"}

        return get(url, {"select": select, "remove": remove, "resolve_links": resolve_links}, headers)
    
    def call_bitcode_method(self, 
                            method: str, 
                            library_id: Optional[str]=None, 
                            object_id: Optional[str]=None, 
                            version_hash: Optional[str]=None, 
                            write_token: Optional[str]=None,
                            params: Dict[str, Any]={},
                            representation: bool=False,
                            host: Optional[str]=None) -> Any:
        if not self.token:
            raise Exception("No token available")
        id = write_token or version_hash or object_id
        if not id:
            raise Exception("Object ID, Version Hash, or Write Token must be specified")
        call_type = 'rep' if representation else 'call'
        if not library_id:
            library_id = self.content_object_library_id(object_id, version_hash)
        path = build_url('qlibs', library_id, 'q', id, call_type, method)
        if host is None:
            host = self._get_host()
        url = build_url(host, path)
        headers = {"Authorization": f"Bearer {self.token}"}

        return post(url, params, headers)
    
    # Search on a given index object
    def search(self, 
                query: Dict[str, Any],
                library_id: Optional[str]=None, 
                object_id: Optional[str]=None,
                version_hash: Optional[str]=None,
                write_token: Optional[str]=None
               ) -> Any:
        assert query is not None, "Query must be specified"
        if not self.token:
            raise Exception("No token available")
        host = self._get_search_host()
        return self.call_bitcode_method("search", library_id=library_id, object_id=object_id, version_hash=version_hash, write_token=write_token, params=query, host=host, representation=True)
    
    def content_object_library_id(self, 
                       object_id: Optional[str]=None, 
                       version_hash: Optional[str]=None,
                       write_token: Optional[str]=None
                       ) -> str:
        return self.content_object(object_id, version_hash, write_token)["qlib_id"]

    def content_object(self,
                        object_id: Optional[str]=None,
                        version_hash: Optional[str]=None,
                        write_token: Optional[str]=None,
                        library_id: Optional[str]=None) -> Dict[str, str]:
        if not self.token:
            raise Exception("No token available")
        url = self._get_host()
        if not object_id and not version_hash:
            raise Exception("Object ID or Version Hash must be specified")
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        id = write_token or version_hash or object_id
        if not id:
            raise Exception("Object ID, Version Hash, or Write Token must be specified")
        url = build_url(url, 'q', id)
        headers = {"Authorization": f"Bearer {self.token}"}
        return get(url, headers=headers)
    
    def content_object_versions(self,
                       object_id: str,
                       library_id: str) -> Dict[str, Any]:
        if not self.token:
            raise Exception("No token available")
        url = self._get_host()
        if not library_id:
            raise Exception("Library ID must be specified for listing content versions")
        url = build_url(url, 'qlibs', library_id, 'qid', object_id)
        headers = {"Authorization": f"Bearer {self.token}"}
        return get(url, headers=headers)
    
    def download_part(self,
                    part_hash: str,
                    save_path: str, 
                    library_id: Optional[str]=None,
                    object_id: Optional[str]=None,
                    version_hash: Optional[str]=None,
                    write_token: Optional[str]=None) -> None:
        if not self.token:
            raise Exception("No token available")
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        id = write_token or version_hash or object_id
        if not id:
            raise Exception("Object ID, Version Hash, or Write Token must be specified")
        url = build_url(url, 'q', id, 'rep', 'parts_download')
        params = {"part_hash": part_hash}
        response = requests.get(url, params=params, headers={"Authorization": f"Bearer {self.token}"})
        if response.status_code == 200:
            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  
                        file.write(chunk)
        else:
            response.raise_for_status()

    def merge_metadata(self,
                    write_token: str,
                    metadata: Any,
                    library_id: str,
                    metadata_subtree: Optional[str]=None) -> Any:
        url = self._get_host()
        url = build_url(url, 'qlibs', library_id, 'q', write_token, 'meta')
        if metadata_subtree:
            url = build_url(url, metadata_subtree)
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json", "Content-Type": "application/json"}
        response = requests.post(url, headers=headers, json=metadata)
        response.raise_for_status()
    
    def replace_metadata(self,
                       write_token: str,
                       metadata: Any,
                       library_id: str,
                       metadata_subtree: Optional[str]=None) -> Any:
        url = self._get_host()
        url = build_url(url, 'qlibs', library_id, 'q', write_token, 'meta')
        if metadata_subtree:
            url = build_url(url, metadata_subtree)
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json", "Content-Type": "application/json"}
        response = requests.put(url, headers=headers, json=metadata)
        response.raise_for_status()

    @dataclass
    class FileJob:
        local_path: str
        out_path: str
        mime_type: str

    def upload_files(self,
                        write_token: str,
                        library_id: str,
                        file_jobs: List[FileJob]) -> None:
        url = self._get_host()
        url = build_url(url, 'qlibs', library_id, 'q', write_token, 'file_jobs')
        headers = {"Authorization": f"Bearer {self.token}",
                   "Accept": "application/json",
                   "Content-Type": "application/json"}
        ops = [{"type": "file", "path": job.out_path, "mime_type": job.mime_type, "size": os.path.getsize(job.local_path)} for job in file_jobs]
        response = requests.post(url, headers=headers, json={"ops": ops})
        response.raise_for_status()
        job_data = response.json()
        job_id, file_job_id = job_data["id"], job_data["jobs"][0]
        upload_url = build_url(url, job_id, file_job_id)
        headers = {"Authorization": f"Bearer {self.token}",
                   "Accept": "application/json",
                   "Content-Type": "application/octet-stream"}
        for job in file_jobs:
            with open(job.local_path, 'rb') as file:
                response = requests.post(upload_url, headers=headers, data=file)
                response.raise_for_status()
        url = build_url(self._get_host(), 'qlibs', library_id, 'q', write_token, 'files')
        response = requests.post(url, headers={"Authorization": f"Bearer {self.token}"})
        response.raise_for_status()

    def list_files(self,
                    library_id: Optional[str]=None,
                    object_id: Optional[str]=None,
                    version_hash: Optional[str]=None,
                    write_token: Optional[str]=None,
                    path: Optional[str]=None) -> Any:
        id = write_token or version_hash or object_id
        if not id:
            raise Exception("Object ID, Version Hash, or Write Token must be specified")
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        url = build_url(url, 'q', id, 'files_list')
        if path:
            url = build_url(url, path)
        headers = {"Authorization": f"Bearer {self.token}"}
        return get(url, headers=headers)
    
    def download_file(self,
                        file_path: str,
                        dest_path: str,
                        library_id: Optional[str]=None,
                        object_id: Optional[str]=None,
                        version_hash: Optional[str]=None,
                        write_token: Optional[str]=None,
    ) -> None:
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        id = write_token or version_hash or object_id
        url = build_url(url, 'q', id, 'files', file_path)
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 200:
            with open(dest_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        else:
            response.raise_for_status()