
from typing import Any, Dict
from typing import List, Optional, Tuple
from loguru import logger
import requests
from requests.exceptions import HTTPError
from dataclasses import dataclass
import os
import aiohttp
import asyncio
from datetime import datetime
from urllib.parse import quote

from .utils import get, build_url, post, get_from_path
from .config import config

class ElvClient():
    def __init__(self, fabric_uris: List[str], search_uris: List[str]=[], static_token: str=""):
        self.fabric_uris = fabric_uris
        self.search_uris = search_uris
        self.token = static_token
        self.semaphore = asyncio.Semaphore(config["client"]["max_concurrent_requests"])

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
        url = build_url(url, 'q', id, 'meta', quote(metadata_subtree))
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
                    metadata_subtree: Optional[str]=None) -> None:
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
                       metadata_subtree: Optional[str]=None) -> None:
        url = self._get_host()
        url = build_url(url, 'qlibs', library_id, 'q', write_token, 'meta')
        if metadata_subtree:
            url = build_url(url, quote(metadata_subtree))
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
                    file_jobs: List[FileJob],
                    finalize: bool=True) -> None:
        # strip leading slashes
        file_jobs = file_jobs[:]
        for job in file_jobs:
            if job.out_path.startswith("/"):
                job.out_path = job.out_path[1:]

        path_to_job = {job.out_path: job for job in file_jobs}

        # Create upload job
        url = build_url(self._get_host(), 'qlibs', library_id, 'q', write_token, 'file_jobs')
        headers = {"Authorization": f"Bearer {self.token}",
                   "Accept": "application/json",
                   "Content-Type": "application/json"}
        ops = [{"type": "file", "path": job.out_path, "mime_type": job.mime_type, "size": os.path.getsize(job.local_path)} for job in file_jobs]
        response = requests.post(url, headers=headers, json={"ops": ops})
        try:
            response.raise_for_status()
        except HTTPError as e:
            logger.error(f"Failed to create file jobs: {e}")
            logger.error(response.text)
            if response.status_code == 409:
                raise ValueError("Provided write token has already been used to upload files")
            raise e
        job_data = response.json()
        job_id, file_job_id = job_data["id"], job_data["jobs"][0]
        assert len(job_data["jobs"]) == 1, "Expected only one file job"

        url = build_url(self._get_host(), 'qlibs', library_id, 'q', write_token, 'file_jobs', job_id, 'uploads', file_job_id)
        next_start = 0
        ordered_paths = []
        # iterate through the pages of file jobs
        while next_start != -1:
            response = requests.get(url, params={"start": next_start}, headers={"Authorization": f"Bearer {self.token}"})
            response.raise_for_status()
            file_info = response.json()
            next_start = file_info["next"]
            ordered_paths.extend(file["path"] for file in file_info["files"])

        # load files into single buffer
        data_buffer = bytearray()
        for path in ordered_paths:
            job = path_to_job[path]
            with open(job.local_path, 'rb') as file:
                data_buffer += file.read()

        # upload buffer
        upload_url = build_url(self._get_host(), 'qlibs', library_id, 'q', write_token, 'file_jobs', job_id, file_job_id)
        headers = {"Authorization": f"Bearer {self.token}",
                   "Accept": "application/json",
                   "Content-Type": "application/octet-stream"}
        response = requests.post(upload_url, headers=headers, data=data_buffer)
        try:
            response.raise_for_status()
        except HTTPError as e:
            logger.error(f"Failed to upload file {job.local_path}: {e}")
            logger.error(response.text)
            raise e

        if finalize:
            self.finalize_files(write_token, library_id)
            
    def finalize_files(self,
                       write_token: str,
                       library_id: str) -> None:
        # finalize upload, write token cannot be used to upload more files after this
        url = build_url(self._get_host(), 'qlibs', library_id, 'q', write_token, 'files')
        response = requests.post(url, headers={"Authorization": f"Bearer {self.token}"})
        try:
            response.raise_for_status()
        except HTTPError as e:
            logger.error(f"Failed to finalize file upload: {e}")
            logger.error(response.text)
            raise e

    def list_files(self,
                    library_id: Optional[str]=None,
                    object_id: Optional[str]=None,
                    version_hash: Optional[str]=None,
                    write_token: Optional[str]=None,
                    path: Optional[str]="/") -> List[str]:
        if path.startswith("/"):
            path = path[1:]
        if path.endswith("/"):
            path = path[:-1]
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
        response = get(url, headers=headers)
        response = get_from_path(response, path)
        result = []
        for entry, info in response.items():
            if entry == ".":
                continue
            if "type" in info["."] and info["."]["type"] == "directory":
                result.append(entry + "/")
            else:
                result.append(entry)
        return result
    
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
        url = build_url(url, 'q', id, 'files', quote(file_path))
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 200:
            with open(dest_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        else:
            response.raise_for_status()

    async def download_file_async(self,
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
        url = build_url(url, 'q', id, 'files', quote(file_path))
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        headers = {"Authorization": f"Bearer {self.token}"}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with self.semaphore:
                async with session.get(url) as response:
                    if response.status == 200:
                        with open(dest_path, "wb") as file:
                            async for chunk in response.content.iter_chunked(8192):
                                file.write(chunk)
                    else:
                        response.raise_for_status()

    def download_directory(self,
                        dest_path: str,
                        fabric_path: Optional[str]="/",
                        library_id: Optional[str]=None,
                        object_id: Optional[str]=None,
                        version_hash: Optional[str]=None,
                        write_token: Optional[str]=None,
    ) -> None:
        if not fabric_path.startswith("/"):
            fabric_path = f"/{fabric_path}"
        if not fabric_path.endswith("/"):
            fabric_path += "/"
        if not os.path.exists(dest_path):
            os.makedirs(dest_path)

        def crawl_files(path: str) -> List[str]:
            """
            Recursively crawls fabric files rooted at path and returns a list of all files
            """
            entries = self.list_files(library_id, object_id, version_hash, write_token, path)
            result = []
            for entry in entries:
                if entry.endswith("/"):
                    result += crawl_files(f"{path}{entry}")
                else:
                    result.append(f"{path}{entry}")
            return result

        paths = crawl_files(fabric_path)

        return self.download_files([(path, path.removeprefix(fabric_path)) for path in paths], dest_path, library_id, object_id, version_hash, write_token)
        
    def download_files(
                    self,
                    file_jobs: List[Tuple[str, str]],
                    dest_path: str,
                    library_id: Optional[str]=None,
                    object_id: Optional[str]=None,
                    version_hash: Optional[str]=None,
                    write_token: Optional[str]=None,
    ) -> None:
        if not os.path.exists(dest_path):
            os.makedirs(dest_path)
        
        # Asynchronous function to handle multiple requests
        async def fetch_all(fabric_file_path: List[str]):
            tasks = []
            for fpath, out_path in fabric_file_path:
                tasks.append(self.download_file_async(fpath, os.path.join(dest_path, out_path), \
                                                      library_id=library_id, \
                                                      object_id=object_id,
                                                      version_hash=version_hash,
                                                      write_token=write_token))
            return await asyncio.gather(*tasks)

        asyncio.run(fetch_all(file_jobs))

    def set_commit_message(self, write_token: str, message: str, library_id: str) -> None:
        commit_data = {"commit": {"message": message, "timestamp": datetime.now().isoformat(timespec='microseconds') + 'Z'}}
        self.merge_metadata(write_token, commit_data, library_id=library_id)
