from typing import Any, Dict, Tuple
import json

import base58
import requests


def decode_version_hash(version_hash: str) -> Dict[str, str]:
    if not (version_hash.startswith("hq__") or version_hash.startswith("tq")):
        raise ValueError(f"Invalid version hash: {version_hash}")
    version_hash = version_hash[4:]
    bytes = base58.b58decode(version_hash)
    digest_bytes = bytes[0:32]
    digest = digest_bytes.hex()
    bytes = bytes[32:]

    size, remaining_bytes = varint_decode(bytes)

    # Remaining bytes is object ID
    object_id = "iq__" + base58.b58encode(remaining_bytes).decode("utf-8")

    # Part hash is B58 encoded version hash without the ID
    part_hash = "hqp_" + \
        base58.b58encode(
            digest_bytes + bytes[:len(bytes) - len(remaining_bytes)]).decode("utf-8")

    return {
        "digest": digest,
        "size": size,
        "objectId": object_id,
        "partHash": part_hash
    }
    

def varint_decode(data: bytes) -> Tuple[int, int]:
    result = 0
    shift = 0
    for byte in data:
        result |= (byte & 0x7f) << shift
        if not (byte & 0x80):
            return result, data[data.index(byte) + 1:]
        shift += 7
    return result, data


def hash_to_address(hash: str) -> str:
    hash = hash[4:]
    return format_address(f'0x{base58.b58decode(hash).hex()}')


def format_address(address: str) -> str:
    address = address.strip()

    if not address.startswith("0x"):
        address = f"0x{address}"

    return address.lower()


def address_to_hash(address: str) -> str:
    address = address[2:]
    return base58.b58encode(bytes.fromhex(address)).decode('utf-8')


def address_to_library_id(address: str) -> str:
    return f'ilib{address_to_hash(address)}'


def _request_json(
    method: str,
    url: str,
    params: dict | None = None,
    body: dict | None = None,
    headers: dict | None = None,
) -> Any:
    """
    Perform http request expecting a json response. Raises HTTPError on failure containing the status code, and 
    the json body of response if available.
    """
    response = requests.request(
        method=method,
        url=url,
        params=params,
        json=body,
        headers=headers,
    )
    response.raise_for_status()
    return response.json()

def get_json(
        url: str, 
        params: dict | None = None, 
        headers: dict | None = None
) -> Any:
    return _request_json("GET", url, params=params, headers=headers)


def post_json(
    url: str,
    params: dict | None = None,
    body: dict | None = None,
    headers: dict | None = None,
) -> Any:
    return _request_json("POST", url, params=params, body=body, headers=headers)


def build_url(*args) -> str:
    """Helper to join URL parts."""
    return "/".join(args)


def get_from_path(data, path, delimiter='/'):
    if not path or path == "/":
        return data
    keys = path.split(delimiter)
    for key in keys:
        data = data.get(key)
        if data is None:
            return None
    return data
