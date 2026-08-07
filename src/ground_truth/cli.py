"""Command line script for ground truth pool management.

Adds assets and/or entity metadata to a ground truth pool object, writing to a draft (write token) that is
left open for the caller to finalize.

Examples:
    # What entities does this pool have, and what attributes does its schema declare?
    python -m elv_client_py.ground_truth.cli info --object-id iq__abc

    # New entity, three images, the first one as its representative (anchor) image, can upload more files later to the same draft/entity
    python -m elv_client_py.ground_truth.cli add-assets --object-id iq__abc \\
        --label "Tom Hanks" --meta gender=male --meta role=actor \\
        --file ./tom1.jpg --file ./tom2.jpg --file ./tom3.jpg --anchor tom1.jpg --no-finalize-files
    # if --no-finalize-files is omitted, the draft is sealed and no more files can be added to it, but the entity can still be updated with more metadata using the same write token

    # More images for an entity that already exists, reusing an open draft
    python -m elv_client_py.ground_truth.cli add-assets --object-id iq__abc \\
        --entity-id 4Xg... --file ./tom4.jpg --write-token tqw__xyz

The fabric auth token is read from --auth or the ELV_AUTH_TOKEN environment variable.
It is the "bearer" key value in the response from the command 
`elv content token create <object_id> --update --secret <PRIVATE_KEY> --config-url https://main.net955305.contentfabric.io/config`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from loguru import logger

from ..elv_client import ElvClient
from .errors import BadRequestException
from .model import (
    DEFAULT_FILE_PATH,
    AddAssetsRequest,
    AssetInput,
    EntityInput,
)
from .service import GroundTruthService

DEFAULT_CONFIG_URL = "https://main.net955305.contentfabric.io/config"


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    token = args.auth or os.getenv("ELV_AUTH_TOKEN")
    if not token:
        parser.error("No auth token: pass --auth or set ELV_AUTH_TOKEN")

    client = ElvClient.from_configuration_url(args.config_url, token, timeout=args.timeout)
    service = GroundTruthService(client)

    try:
        if args.command == "info":
            result = service.pool_info(args.object_id)
        elif args.command == "write-token":
            result = service.create_write_token(args.object_id)
        elif args.command == "finalize-files":
            result = service.finalize_files(args.write_token, args.library_id)
        else:
            result = service.add_assets(_build_add_assets_request(args))
    except BadRequestException as exc:
        logger.error(str(exc))
        return 2

    print(json.dumps(asdict(result), indent=2))
    return 0


def _build_add_assets_request(args: argparse.Namespace) -> AddAssetsRequest:
    """Turn parsed arguments into a service request, matching files to their metadata by name."""
    asset_metadata = _load_asset_metadata(args.assets_json)
    unmatched = set(asset_metadata) - {os.path.basename(path) for path in args.file}

    if unmatched:
        raise BadRequestException(
            f"--assets-json has entries for files that were not passed with --file: "
            f"{', '.join(sorted(unmatched))}")
    if args.anchor and args.anchor not in {os.path.basename(path) for path in args.file}:
        raise BadRequestException(f"--anchor names a file that was not passed with --file: {args.anchor}")

    assets = []
    for path in args.file:
        filename = os.path.basename(path)
        metadata = asset_metadata.get(filename, {})
        assets.append(AssetInput(
            local_path=path,
            filename=filename,
            label=metadata.get("label", ""),
            description=metadata.get("description", ""),
            source=metadata.get("source", ""),
            anchor=bool(metadata.get("anchor")) or filename == args.anchor,
            mime_type=metadata.get("mime_type")
        ))

    return AddAssetsRequest(
        object_id=args.object_id,
        entity=EntityInput(
            entity_id=args.entity_id,
            label=args.label,
            description=args.description,
            meta=_parse_meta(args.meta)
        ),
        assets=assets,
        file_path=args.file_path,
        commit_message=args.commit_message,
        write_token=args.write_token,
        library_id=args.library_id,
        finalize_files=not args.no_finalize_files
    )


def _load_asset_metadata(value: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """Parse --assets-json (inline JSON or @path) into per-filename metadata."""
    if not value:
        return {}

    if value.startswith("@"):
        with open(value[1:]) as file:
            entries = json.load(file)
    else:
        entries = json.loads(value)

    if not isinstance(entries, list):
        raise BadRequestException("--assets-json must be a JSON array of asset objects")

    metadata = {}
    for entry in entries:
        filename = entry.get("filename")
        if not filename:
            raise BadRequestException("Every --assets-json entry needs a 'filename'")
        metadata[filename] = entry

    return metadata


def _parse_meta(values: List[str]) -> Optional[Dict[str, str]]:
    """Parse repeated --meta key=value pairs."""
    if not values:
        return None

    meta = {}
    for value in values:
        key, separator, attribute = value.partition("=")
        if not separator or not key:
            raise BadRequestException(f"--meta must be key=value, got: {value}")
        meta[key.strip()] = attribute.strip()

    return meta


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="elv-ground-truth",
        description="Manage assets in an Eluvio ground truth pool.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--config-url", default=os.getenv("ELV_CONFIG_URL", DEFAULT_CONFIG_URL),
                        help="Fabric configuration URL (env ELV_CONFIG_URL)")
    parser.add_argument("--auth", help="Fabric auth token (env ELV_AUTH_TOKEN)")
    parser.add_argument("--timeout", type=float, default=60.0, help="Request timeout in seconds")
    parser.add_argument("--verbose", action="store_true", help="Debug logging")

    commands = parser.add_subparsers(dest="command", required=True)

    info = commands.add_parser("info", help="Show a pool's schema and entities")
    info.add_argument("--object-id", required=True, help="Ground truth pool object ID")

    write_token = commands.add_parser(
        "write-token", help="Create a draft of the pool object and print its write token")
    write_token.add_argument("--object-id", required=True, help="Ground truth pool object ID")

    add_assets = commands.add_parser(
        "add-assets", help="Upload assets and/or write entity metadata to a pool")
    add_assets.add_argument("--object-id", required=True, help="Ground truth pool object ID")
    add_assets.add_argument("--library-id", help="Library ID of the pool object (looked up if omitted)")
    add_assets.add_argument("--entity-id", help="Add to this existing entity")
    add_assets.add_argument("--label", help="Entity label (required when creating a new entity)")
    add_assets.add_argument("--description", help="Entity description")
    add_assets.add_argument("--meta", action="append", default=[], metavar="KEY=VALUE",
                            help="Entity attribute, repeatable. Must be declared by the pool's schema")
    add_assets.add_argument("--file", action="append", default=[], metavar="PATH",
                            help="Image file to upload, repeatable")
    add_assets.add_argument("--assets-json", metavar="JSON|@PATH",
                            help="Per-file metadata: JSON array of "
                                 "{filename, label, description, source, anchor, mime_type}")
    add_assets.add_argument("--anchor", metavar="FILENAME",
                            help="Which uploaded file becomes the entity's representative image")
    add_assets.add_argument("--file-path", default=DEFAULT_FILE_PATH, metavar="DIR",
                            help=f"Directory in the object's file bundle (default: {DEFAULT_FILE_PATH})")
    add_assets.add_argument("--commit-message", help="Commit message to set on the draft")
    add_assets.add_argument("--write-token", help="Write to this existing draft instead of creating one")
    add_assets.add_argument("--no-finalize-files", action="store_true",
                            help="Leave the draft's file upload bundle open so later runs can "
                                 "upload to the same draft. Seal it with the finalize-files "
                                 "command before finalizing the object")

    finalize_files = commands.add_parser(
        "finalize-files", help="Seal a draft's file upload bundle (does not commit the object)")
    finalize_files.add_argument("--write-token", required=True, help="Draft to seal")
    finalize_files.add_argument("--library-id", help="Library ID of the pool object (looked up if omitted)")

    return parser


if __name__ == "__main__":
    sys.exit(main())
