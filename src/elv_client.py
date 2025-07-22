
import asyncio
import os
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import quote

import aiohttp
import requests
from loguru import logger
from requests.exceptions import HTTPError
from tqdm import tqdm

from .config import config
from .utils import build_url, get_json, get_from_path, post_json


class ElvClient():
    """Client for using the Eluvio Content Fabric API.
    """

    def __init__(self, fabric_uris: List[str], search_uris: List[str], static_token: str = ""):
        self.fabric_uris = fabric_uris
        self.search_uris = search_uris
        self.token = static_token

        self.semaphore = asyncio.Semaphore(
            config["client"]["max_concurrent_requests"])
        self.loop = asyncio.new_event_loop()
        self.thread_id = threading.get_ident()

    @staticmethod
    def from_configuration_url(config_url: str, static_token: str) -> 'ElvClient':
        """Create an ElvClient instance from a fabric config url and an auth token.

        Example:
        ```python
        client = ElvClient.from_configuration_url(
            "main.net955305.contentfabric.io/config", "<AUTH_TOKEN>")
        ```
        """

        fconfig = get_json(config_url)
        services = fconfig.get("network", {}).get("services", {})
        if len(services) == 0:
            raise ValueError("No services available in the configuration")
        if "fabric_api" not in services:
            raise ValueError("No Fabric URIs available in the configuration")
        fabric_uris = services["fabric_api"]
        if not fabric_uris:
            raise ValueError("No Fabric URIs available in the configuration")
        search_uris = services.get("search_v2", [])
        if not search_uris:
            logger.warning("No Search URIs available in the configuration")
        return ElvClient(fabric_uris, search_uris, static_token)

    def set_static_token(self, token: str):
        """Set a static token for the client to use for all requests."""
        self.token = token

    def _get_host(self) -> str:
        if len(self.fabric_uris) == 0:
            raise ValueError("No Fabric URIs available")
        return self.fabric_uris[0]

    def _get_search_host(self) -> str:
        if len(self.search_uris) == 0:
            raise ValueError("No Search URIs available")
        return self.search_uris[0]

    def content_object_metadata(
        self,
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
        metadata_subtree: str = "",
        select: Optional[str] = None,
        remove: Optional[str] = None,
        resolve_links: bool = False
    ) -> Any:
        """Fetch metadata for a content object."""

        if not self.token:
            raise ValueError("No token available")

        url = self._get_host()

        qhit = write_token or version_hash or object_id
        if not qhit:
            raise ValueError(
                "Object ID, Version Hash, or Write Token must be specified")

        if library_id:
            url = build_url(url, 'qlibs', library_id)

        url = build_url(url, 'q', qhit, 'meta', quote(metadata_subtree))

        return get_json(
            url,
            {
                "select": select,
                "remove": remove,
                "resolve": resolve_links,
                "authorization": self.token,
            }
        )

    def call_bitcode_method(
        self,
        method: str,
        params: Dict[str, Any],
        method_type: Literal["GET", "POST"] = "POST",
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
        representation: bool = False,
        host: Optional[str] = None
    ) -> Any:
        """Call bitcode method on a content object.

        Args:
            method (str): The bitcode method to call.
            params (Dict[str, Any]): Parameters to pass to the method. If method_type is GET, this will be used as query parameters
                if method_type is POST, this will be used as the request body.
            method_type (Literal["GET", "POST"]): The HTTP method to use for the request.
            library_id (Optional[str]): The library ID of the content object.
            object_id (Optional[str]): The object ID of the content object.
            version_hash (Optional[str]): The version hash of the content object.
            write_token (Optional[str]): The write token of the content object.
            representation (bool): use /rep instead of /call
            host (Optional[str]): Host URL to use for the request.
        """

        if not self.token:
            raise ValueError("No token available")

        qhit = write_token or version_hash or object_id
        if not qhit:
            raise ValueError(
                "Object ID, Version Hash, or Write Token must be specified")

        bc_type = 'rep' if representation else 'call'
        if not library_id:
            library_id = self.content_object_library_id(
                object_id, version_hash, write_token)

        path = build_url('qlibs', library_id, 'q', qhit, bc_type, method)

        if host is None:
            host = self._get_host()

        url = build_url(host, path)

        if method_type == "GET":
            return get_json(url, params={"authorization": self.token, **params})
        
        return post_json(url, body=params, params={"authorization": self.token})

    def search(
        self,
        query: Dict[str, Any],
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
        use_post: bool = False
    ) -> dict[str, Any]:
        """Search an index

        Args:
            query (Dict[str, Any]): The search query parameters.
            library_id (Optional[str]): The library ID of the index object.
            object_id (Optional[str]): The object ID of the index object.
            version_hash (Optional[str]): The version hash of the index object.
            write_token (Optional[str]): The write token of the index object.
        """

        if not self.token:
            raise ValueError("No token available")
        host = self._get_search_host()
        method_type = "POST" if use_post else "GET"
        return self.call_bitcode_method("search", method_type=method_type, library_id=library_id, object_id=object_id, version_hash=version_hash, write_token=write_token, params=query, host=host, representation=True)

    def crawl(
        self,
        write_token: str,
        library_id: Optional[str] = None,
    ) -> dict:
        """Initiates crawl against the write token on a search host.

        Args:
            write_token(str): write token of index object
            library_id(Optional[str], optional): library id. Defaults to None.

        Returns:
            dict: lro handle
        """

        if not self.token:
            raise ValueError("No token available")
        url = self._get_search_host()
        return self.call_bitcode_method(
            "search_update",
            method_type="POST",
            library_id=library_id,
            write_token=write_token,
            params={},
            host=url,
            representation=False
        )

    def update_site(
        self,
        site_qwt: str,
        ids_to_add: List[str],
        ids_to_remove: List[str],
        replace_all: bool = False,
        site_path: str = "site_map/searchables",
        item_subpath: str = "meta"
    ) -> dict:
        """Update the site with the given IDs.

        Args:
            site_qwt(str): write token of the site object to update
            ids_to_add(List[str]): ids to add to the site, ids already present will be ignored
            ids_to_remove(List[str]): a set of ids to remove from the site
            replace_all(bool, optional): if True, all ids will be replaced with the new ids. Defaults to False.
            site_path(str, optional): path in the site object to the site map. Defaults to "/site_map/searchables".
            item_subpath(str, optional): starting path in the item to crawl, defaults to "meta" which means start at the root.
                If you want to start at "searchables" for instance specify "meta/searchables"

        Returns:
            status
        """

        current_ids, invalid_links = self._get_current_ids(site_qwt, site_path)

        if replace_all:
            all_qids = set(ids_to_add)
        else:
            all_qids = set(current_ids) - set(ids_to_remove) | set(ids_to_add)

        if len(all_qids) == 0:
            raise ValueError("Site has no qids")

        all_qids = sorted(all_qids)

        links = {}
        failed = []
        idx = 1
        for qid in tqdm(all_qids, desc="Adding links"):
            try:
                link = self._get_link(qid, item_subpath)
            except Exception as e:
                logger.error(f"Failed to get link for {qid}: {e}")
                failed.append(qid)
                continue
            links[str(idx)] = link
            idx += 1

        qlib = self.content_object_library_id(object_id=site_qwt)
        self.set_commit_message(site_qwt, "Updated site map", qlib)

        self.replace_metadata(
            write_token=site_qwt,
            metadata=links,
            library_id=qlib,
            metadata_subtree=site_path,
        )

        warnings = []
        if len(failed) > 0:
            warnings.append(f"Failed to add some contents to the site map: [{', '.join(failed)}]")
        if len(invalid_links) > 0:
            warnings.append(f"Found some invalid links in the site map: [{', '.join(invalid_links)}]")

        if warnings:
            msg = "Some issues encountered while updating the site"
        else:
            msg = "Site updated successfully"
        
        return {"message": msg, "warnings": warnings}

    def _get_link(self, qid: str, path: str) -> dict:
        """Get a link to the most recent version of the given qid, at the given metadata path."""
        latest_version = self.content_object(object_id=qid)["hash"]
        return {"/": f"/qfab/{latest_version}/{path}"}

    def _get_current_ids(
        self,
        site_qwt: str,
        site_path: str = "/site_map/searchables"
    ) -> tuple[list[str], list[str]]:
        """Get the current IDs from the site."""
        try:
            site_map = self.content_object_metadata(
                write_token=site_qwt, metadata_subtree=site_path, resolve_links=False)
        except HTTPError:
            logger.info(f"Found no objects in site for {site_qwt}")
            return [], []

        current_ids, invalid_links = [], []
        for k, link in site_map.items():
            try:
                qhash = link["/"].split('/')[2]
                qid = self.content_object(qhash)["id"]
            except Exception as e:
                logger.error(f"Bad link for {k}: {link}\nError: {e}")
                invalid_links.append(k)
                continue
            current_ids.append(qid)

        return current_ids, invalid_links

    def crawl_status(
        self,
        write_token: str,
        lro_handle: str,
        library_id: Optional[str] = None,
    ) -> dict:
        """Checks the status of a crawl operation.
        Args:
            write_token(str): write token of index object
            lro_handle(str): lro handle of the crawl operation
            library_id(Optional[str], optional): library id. Defaults to None.
        Returns:
            dict: status of the crawl operation
        """
        if not self.token:
            raise ValueError("No token available")
        url = self._get_search_host()
        return self.call_bitcode_method("crawl_status",
                                        library_id=library_id,
                                        write_token=write_token,
                                        params={"lro_handle": lro_handle},
                                        host=url,
                                        representation=False)

    def content_object_library_id(
        self,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None
    ) -> str:
        """Get the library ID of a content object."""
        return self.content_object(object_id, version_hash, write_token)["qlib_id"]

    def content_object(
        self,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
        library_id: Optional[str] = None
    ) -> Dict[str, str]:
        """Fetch basic content object information."""

        if not self.token:
            raise ValueError("No token available")
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        qhit = write_token or version_hash or object_id
        if not qhit:
            raise ValueError(
                "Object ID, Version Hash, or Write Token must be specified")
        url = build_url(url, 'q', qhit)
        return get_json(url, params={"authorization": self.token})

    def content_object_versions(
        self,
        object_id: str,
        library_id: str
    ) -> Dict[str, Any]:
        """Fetch list of content object versions for the qid."""
        if not self.token:
            raise ValueError("No token available")
        url = self._get_host()
        if not library_id:
            raise ValueError(
                "Library ID must be specified for listing content versions")
        url = build_url(url, 'qlibs', library_id, 'qid', object_id)
        return get_json(url, params={"authorization": self.token})

    def download_part(
        self,
        part_hash: str,
        save_path: str,
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None
    ) -> None:
        """Download a part

        Args:
            part_hash (str): The hash of the part to download.
            save_path (str): The path where the part should be saved.
            library_id (Optional[str]): The library ID of the content object.
            object_id (Optional[str]): The object ID of the content object.
            version_hash (Optional[str]): The version hash of the content object.
            write_token (Optional[str]): The write token of the content object.
        """

        if self._is_encrypted(part_hash):
            self._download_encrypted_part(
                part_hash, save_path, library_id, object_id, version_hash, write_token)
        else:
            self._download_unencrypted_part(
                part_hash, save_path, library_id, object_id, version_hash, write_token)

    def _download_encrypted_part(
        self,
        part_hash: str,
        save_path: str,
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None
    ) -> None:
        if not self.token:
            raise ValueError("No token available")
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        qhit = write_token or version_hash or object_id
        if not qhit:
            raise ValueError(
                "Object ID, Version Hash, or Write Token must be specified")
        url = build_url(url, 'q', qhit, 'rep', 'parts_download')
        params = {"part_hash": part_hash, "authorization": self.token}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
        else:
            response.raise_for_status()

    def _download_unencrypted_part(
        self,
        part_hash: str,
        save_path: str,
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None
    ) -> None:
        if not self.token:
            raise ValueError("No token available")
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        id = write_token or version_hash or object_id
        if not id:
            raise ValueError(
                "Object ID, Version Hash, or Write Token must be specified")
        url = build_url(url, 'q', id, 'data', part_hash)
        params = {"authorization": self.token}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
        else:
            response.raise_for_status()

    def _is_encrypted(self, part_hash: str) -> bool:
        return part_hash.startswith("hqpe")

    def merge_metadata(
        self,
        write_token: str,
        metadata: Any,
        library_id: str,
        metadata_subtree: Optional[str] = None
    ) -> None:
        """Merge metadata into a content object. Overwrites only leaves which conflict with provided metadata.

        Args:
            write_token (str): Write token of the content object.
            metadata (Any): Metadata to merge into the content object.
            library_id (str): Library ID of the content object.
            metadata_subtree (Optional[str]): Subtree in the metadata to merge into. If None, merges at the root.
        """
        url = self._get_host()
        url = build_url(url, 'qlibs', library_id, 'q', write_token, 'meta')
        if metadata_subtree:
            url = build_url(url, metadata_subtree)
        headers = {"Accept": "application/json",
                   "Content-Type": "application/json"}
        response = requests.post(
            url,
            params={"authorization": self.token},
            headers=headers,
            json=metadata
        )

        response.raise_for_status()

    def replace_metadata(
        self,
        write_token: str,
        metadata: Any,
        library_id: str,
        metadata_subtree: Optional[str] = None
    ) -> None:
        """Replace metadata in a content object. Overwrites all meta rooted at the specified subtree.
        Args:
            write_token (str): Write token of the content object.
            metadata (Any): Metadata to replace in the content object.
            library_id (str): Library ID of the content object.
            metadata_subtree (Optional[str]): Subtree in the metadata to replace. If None, replaces at the root.
        """
        url = self._get_host()
        url = build_url(url, 'qlibs', library_id, 'q', write_token, 'meta')
        if metadata_subtree:
            url = build_url(url, quote(metadata_subtree))
        headers = {"Accept": "application/json",
                   "Content-Type": "application/json"}
        response = requests.put(
            url,
            params={"authorization": self.token},
            headers=headers,
            json=metadata
        )
        response.raise_for_status()

    @dataclass
    class FileJob:
        """Data class representing a file job for uploading files to a content object."""
        local_path: str
        out_path: str
        mime_type: str

    def upload_files(
        self,
        write_token: str,
        library_id: str,
        file_jobs: List[FileJob],
        finalize: bool = True
    ) -> None:
        """Uploads files to a content object.
        Args:
            write_token (str): Write token of the content object.
            library_id (str): Library ID of the content object.
            file_jobs (List[FileJob]): List of FileJob objects containing local paths, output paths, and MIME types.
            finalize (bool): If True, finalizes the upload after all files are uploaded.

        Example:
            ```python
            client = ElvClient.from_configuration_url("main.net955305.contentfabric.io/config", "<AUTH_TOKEN>")
            file_jobs = [
                ElvClient.FileJob(local_path="path/to/local/file1.txt", out_path="file1.txt", mime_type="text/plain"),
                ElvClient.FileJob(local_path="path/to/local/file2.jpg", out_path="images/file2.jpg", mime_type="image/jpeg")
            ]
            client.upload_files(write_token="your_write_token", library_id="your_library_id", file_jobs=file_jobs)
            ```
        """
        # strip leading slashes
        file_jobs = file_jobs[:]
        for job in file_jobs:
            if job.out_path.startswith("/"):
                job.out_path = job.out_path[1:]

        path_to_job = {job.out_path: job for job in file_jobs}

        # Create upload job
        url = build_url(self._get_host(), 'qlibs', library_id,
                        'q', write_token, 'file_jobs')
        headers = {"Accept": "application/json",
                   "Content-Type": "application/json"}
        ops = [{"type": "file", "path": job.out_path, "mime_type": job.mime_type,
                "size": os.path.getsize(job.local_path)} for job in file_jobs]
        response = requests.post(
            url,
            params={"authorization": self.token},
            headers=headers,
            json={"ops": ops}
        )
        try:
            response.raise_for_status()
        except HTTPError as e:
            logger.error(f"Failed to create file jobs: {e}")
            logger.error(response.text)
            if response.status_code == 409:
                raise ValueError(
                    "Provided write token has already been used to upload files") from e
            raise e
        job_data = response.json()
        job_id, file_job_id = job_data["id"], job_data["jobs"][0]
        assert len(job_data["jobs"]) == 1, "Expected only one file job"

        url = build_url(self._get_host(), 'qlibs', library_id, 'q',
                        write_token, 'file_jobs', job_id, 'uploads', file_job_id)
        next_start = 0
        ordered_paths = []
        # iterate through the pages of file jobs
        while next_start != -1:
            response = requests.get(
                url, params={"start": next_start, "authorization": self.token})
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
        upload_url = build_url(self._get_host(
        ), 'qlibs', library_id, 'q', write_token, 'file_jobs', job_id, file_job_id)
        headers = {"Accept": "application/json",
                   "Content-Type": "application/octet-stream"}
        response = requests.post(upload_url, params={
                                 "authorization": self.token}, headers=headers, data=data_buffer)
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
        """Finalizes the file jobs for a given write token."""
        url = build_url(self._get_host(), 'qlibs',
                        library_id, 'q', write_token, 'files')
        response = requests.post(url, params={"authorization": self.token})
        try:
            response.raise_for_status()
        except HTTPError as e:
            logger.error(f"Failed to finalize file upload: {e}")
            logger.error(response.text)
            raise e

    def list_files(
        self,
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
        path: str = "/",
        get_info: bool = False
    ) -> List[str]:
        """Lists files in a content object.
        Args:
            library_id (Optional[str]): Library ID of the content object.
            object_id (Optional[str]): Object ID of the content object.
            version_hash (Optional[str]): Version Hash of the content object.
            write_token (Optional[str]): Write Token of the content object.
            path (Optional[str]): Path in the content object to list files from. Defaults to "/".
            get_info (bool): If True, returns full response with file sizes and types. Defaults to False.
        Returns:
            List[str]: List of file paths in the content object.
        """
        if path.startswith("/"):
            path = path[1:]
        if path.endswith("/"):
            path = path[:-1]
        id = write_token or version_hash or object_id
        if not id:
            raise ValueError(
                "Object ID, Version Hash, or Write Token must be specified")
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        url = build_url(url, 'q', id, 'files_list')
        if path:
            url = build_url(url, path)
        response = get_json(url, params={"authorization": self.token})
        response = get_from_path(response, path)
        if get_info:
            # return full response which contains file sizes
            return response
        result = []
        for entry, info in response.items():
            if entry == ".":
                continue
            if "type" in info["."] and info["."]["type"] == "directory":
                result.append(entry + "/")
            else:
                result.append(entry)
        return result

    def download_file(
        self,
        file_path: str,
        dest_path: str,
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
    ) -> None:
        """Downloads a file from the content object to a local path."""
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        qhit = write_token or version_hash or object_id
        url = build_url(url, 'q', qhit, 'files', quote(file_path))
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        response = requests.get(
            url,
            params={"authorization": self.token},
            stream=True
        )
        if response.status_code == 200:
            with open(dest_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        else:
            response.raise_for_status()

    async def download_file_async(
        self,
        file_path: str,
        dest_path: str,
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
    ) -> Optional[Exception]:
        """Downloads a file from the content object to a local path asynchronously.

        Returns an Optional[ValueError] if an error occurs, or None if successful.
        """
        url = self._get_host()
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        qhit = write_token or version_hash or object_id
        url = build_url(url, 'q', qhit, 'files', quote(file_path))
        try:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        except PermissionError as e:
            return PermissionError(f"Failed to create output directory {os.path.dirname(dest_path)}: {e}")
        async with aiohttp.ClientSession() as session:
            async with self.semaphore:
                async with session.get(url, params={"authorization": self.token}) as response:
                    if response.status != 200:
                        return HTTPError(f"Failed to download file {file_path}: {response.status}, {response.text}")
                    try:
                        with open(dest_path, "wb") as file:
                            async for chunk in response.content.iter_chunked(8192):
                                file.write(chunk)
                    except ValueError as e:
                        return IOError(f"Failed to write file {dest_path}: {e}")
        return None

    def download_directory(
        self,
        dest_path: str,
        fabric_path: str = "/",
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
    ) -> None:
        """Recursively downloads a files directory from the content object to a local path."""
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
            entries = self.list_files(
                library_id, object_id, version_hash, write_token, path)
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
        library_id: Optional[str] = None,
        object_id: Optional[str] = None,
        version_hash: Optional[str] = None,
        write_token: Optional[str] = None,
    ) -> List[Optional[ValueError]]:
        """Downloads a list of files to a destination directory.

        Args:
            file_jobs: List of tuples where each tuple is a pair of(fabric_path, out_path)
            dest_path: Destination directory to save the files
            library_id: Library ID
            object_id: Object ID
            version_hash: Version Hash
            write_token: Write Token

        Returns:
            List of ValueErrors for each file download, or None if successful
        """
        self._check_thread()

        if not os.path.exists(dest_path):
            os.makedirs(dest_path)

        # Asynchronous function to handle multiple requests
        async def fetch_all(fabric_file_path: List[str]):
            tasks = []
            for fpath, out_path in fabric_file_path:
                tasks.append(self.download_file_async(fpath, os.path.join(dest_path, out_path),
                                                      library_id=library_id,
                                                      object_id=object_id,
                                                      version_hash=version_hash,
                                                      write_token=write_token))
            return await asyncio.gather(*tasks, return_exceptions=True)

        return self.loop.run_until_complete(fetch_all(file_jobs))

    def _check_thread(self):
        """Ensure the client is only accessed from the same thread."""
        if threading.get_ident() != self.thread_id:
            raise RuntimeError(
                "ElvClient currently only supports single-threaded access for async operations.")

    def set_commit_message(self, write_token: str, message: str, library_id: str) -> None:
        """Set a commit message for a content object."""
        commit_data = {"commit": {"message": message, "timestamp": datetime.now(
        ).isoformat(timespec='microseconds') + 'Z'}}
        self.merge_metadata(write_token, commit_data, library_id=library_id)

    def __del__(self):
        if self.loop.is_running():
            self.loop.stop()
        self.loop.close()
