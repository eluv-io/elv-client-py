
from typing import Any, Dict, Self
from typing import List

from .utils import get, build_url

class ElvClient():
    def __init__(self, fabric_uris: List[str], search_uris: List[str]=[], static_token: str=""):
        self.fabric_uris = fabric_uris
        self.search_uris = search_uris
        self.token = static_token

    @staticmethod
    def from_configuration_url(config_url: str, static_token: str="") -> Self:
        config = get(config_url)
        fabric_uris = config["network"]["services"]["fabric_api"]
        search_uris = config["network"]["services"]["search_v2"]
        return ElvClient(fabric_uris, search_uris, static_token)
    
    def set_static_token(self, token: str):
        self.token = token

    def _get_host(self) -> str:
        return self.fabric_uris[0]
    
    def _get_search_host(self) -> str:
        return self.search_uris[0]
    
    def content_object_metadata(self, 
                                library_id: str=None, 
                                object_id: str=None, 
                                version_hash: str=None, 
                                metadata_subtree: str=None,
                                select: List[str]=None, 
                                remove: List[str]=None,
                                resolve_links: bool=False
                                ) -> Any:
        host = self._get_host()
        if not library_id:
            library_id = self.content_object_library_id(object_id, version_hash)
        url = build_url(host, 'qlibs', library_id, 'q', version_hash if version_hash else object_id, 'meta', metadata_subtree)
        headers = {"Authorization": f"Bearer {self.token}"}

        return get(url, {"select": select, "remove": remove, "resolve_links": resolve_links}, headers)
    
    def call_bitcode_method(self, 
                            library_id: str=None, 
                            object_id: str=None, 
                            version_hash: str=None, 
                            method: str=None, 
                            params: Dict[str, Any]=None,
                            representation: bool=False,
                            host: str=None) -> Any:
        assert method is not None, "Method must be specified"
        call_type = 'rep' if representation else 'call'
        if not library_id:
            library_id = self.content_object_library_id(object_id, version_hash)
        path = build_url('qlibs', library_id, 'q', version_hash if version_hash else object_id, call_type, method)
        if host is None:
            host = self._get_host()
        url = build_url(host, path)
        headers = {"Authorization": f"Bearer {self.token}"}

        return get(url, params, headers)
    
    # Search on a given index object
    def search(self, 
               library_id: str=None, 
               object_id: str=None,
               version_hash: str=None,
               query: Dict[str, Any]=None) -> Any:
        assert query is not None, "Query must be specified"
        host = self._get_search_host()
        return self.call_bitcode_method(library_id, object_id, version_hash, "search", query, host=host, representation=True)
    
    def content_object_library_id(self, 
                       object_id: str=None, 
                       version_hash: str=None
                       ) -> str:
        return self.content_object(object_id, version_hash)["qlib_id"]

    def content_object(self,
                       object_id: str=None,
                       version_hash: str=None,
                       library_id: str=None) -> Dict[str, str]:
        url = build_url('q', version_hash if version_hash else object_id)
        if library_id:
            url = build_url(url, 'qlibs', library_id)
        url = build_url(self._get_host(), url)
        headers = {"Authorization": f"Bearer {self.token}"}
        return get(url, headers=headers)