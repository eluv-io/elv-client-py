"""Ground truth pool management against the content fabric.

Ports the write flow of the video editor's ``GroundTruthStore`` (elv-video-editor):

  * assets are files in the pool content object, under ``/files/<file_path>/<filename>``
  * each entity lives at ``/ground_truth/entities/<entity_id>`` and links its assets from
    a ``sample_files`` array, e.g. ``{"/": "./files/assets/tom_hanks.jpg"}``
  * uploads and metadata writes share one draft, so a link never points at a file that
    isn't in the same write token

The draft is left open: every method returns its write token and the caller finalizes.
"""

from __future__ import annotations

import mimetypes
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import base58
from loguru import logger
from requests.exceptions import HTTPError

from ..elv_client import ElvClient
from .errors import BadRequestException
from .model import (
    AddAssetsRequest,
    AddAssetsResult,
    AddedAsset,
    AssetInput,
    EntitySummary,
    PoolInfo,
    WriteTokenInfo,
)

GROUND_TRUTH = "ground_truth"
ENTITIES = f"{GROUND_TRUTH}/entities"
DEFAULT_MIME_TYPE = "application/octet-stream"


class GroundTruthService:
    """Pool management operations. One instance per authenticated client."""

    def __init__(self, client: ElvClient):
        self.client = client

    def pool_info(self, object_id: str) -> PoolInfo:
        """Summarize a ground truth pool: its schema and the entities available to add assets to."""
        library_id = self.client.content_object_library_id(object_id=object_id)
        ground_truth = self._read(GROUND_TRUTH, object_id=object_id) or {}
        entities = ground_truth.get("entities") or {}
        schema = ground_truth.get("entity_data_schema") or {}

        return PoolInfo(
            object_id=object_id,
            library_id=library_id,
            name=self._read("public/name", object_id=object_id) or object_id,
            description=self._read("public/description", object_id=object_id) or "",
            model_domain=ground_truth.get("model_domain") or "",
            attributes=_schema_attributes(schema),
            entity_data_schema=schema,
            entities=[
                EntitySummary(
                    id=entity_id,
                    label=(entity or {}).get("label") or entity_id,
                    num_assets=len((entity or {}).get("sample_files") or []),
                )
                for entity_id, entity in sorted(entities.items())
            ],
        )

    def create_write_token(
        self,
        object_id: str,
        library_id: Optional[str] = None
    ) -> WriteTokenInfo:
        """Create a draft of the pool object to write new assets into."""
        library_id = library_id or self.client.content_object_library_id(object_id=object_id)
        write_token = self.client.get_write_token(object_id=object_id, library_id=library_id)
        logger.info(f"Created write token {write_token} for ground truth pool {object_id}")

        return WriteTokenInfo(object_id=object_id, library_id=library_id, write_token=write_token)

    def add_assets(self, request: AddAssetsRequest) -> AddAssetsResult:
        """Add assets and/or entity metadata to a ground truth pool.

        Uploads each asset into the pool object's file bundle, then writes the entity (with its
        ``sample_files`` links) to the draft. Either the files or the entity metadata may be
        omitted, but not both. The returned write token is left open for the caller to finalize.
        """
        entity_input = request.entity
        has_metadata = any(
            value is not None
            for value in (entity_input.label, entity_input.description, entity_input.meta)
        )
        if not request.assets and not has_metadata:
            raise BadRequestException(
                "Nothing to write: provide asset files, entity metadata, or both")

        library_id = request.library_id or self.client.content_object_library_id(
            object_id=request.object_id)
        write_token = request.write_token or self.client.get_write_token(
            object_id=request.object_id, library_id=library_id)

        self._validate_attributes(write_token, entity_input.meta)
        entity_id, existing = self._resolve_entity(write_token, request.object_id, entity_input)

        now = _timestamp()
        sample_files: List[Dict[str, Any]] = list((existing or {}).get("sample_files") or [])
        added = self._upload_assets(write_token, library_id, request)

        anchor_index = next(
            (index for index, asset in enumerate(request.assets) if asset.anchor), None)
        sample_files.extend(added)
        if anchor_index is not None:
            # At most one anchor per entity, so the new one displaces the old.
            for sample_file in sample_files:
                sample_file.pop("anchor", None)
            sample_files[len(sample_files) - len(added) + anchor_index]["anchor"] = True

        entity: Dict[str, Any] = dict(existing or {})
        entity["id"] = entity_id
        if entity_input.label is not None:
            entity["label"] = entity_input.label
        if entity_input.description is not None:
            entity["description"] = entity_input.description
        if entity_input.meta:
            entity["meta"] = {**(entity.get("meta") or {}), **entity_input.meta}
        entity.setdefault("description", "")
        entity.setdefault("meta", {})
        entity.setdefault("added_at", now)
        entity["sample_files"] = sample_files
        entity["updated_at"] = now

        self.client.replace_metadata(
            write_token=write_token,
            library_id=library_id,
            metadata_subtree=f"/{ENTITIES}/{entity_id}",
            metadata=_strip_link_urls(entity)
        )

        commit_message = request.commit_message or _default_commit_message(
            entity["label"], len(added))
        self.client.set_commit_message(
            write_token=write_token, message=commit_message, library_id=library_id)

        logger.info(
            f"Wrote {len(added)} asset(s) and {"some" if has_metadata else "no"} metadata to entity {entity_id} of pool {request.object_id} "
            f"in write token {write_token}"
        )

        return AddAssetsResult(
            object_id=request.object_id,
            library_id=library_id,
            write_token=write_token,
            entity_id=entity_id,
            entity_created=existing is None,
            entity_label=entity["label"],
            assets=[
                AddedAsset(
                    id=sample_file["id"],
                    path=sample_file["link"]["/"].replace("./files/", "", 1),
                    link=sample_file["link"]["/"],
                    label=sample_file["label"],
                    anchor=bool(sample_file.get("anchor"))
                )
                for sample_file in added
            ],
            total_assets=len(sample_files),
            commit_message=commit_message
        )

    def finalize_files(self, write_token: str, library_id: Optional[str] = None) -> WriteTokenInfo:
        """Seal the draft's file upload bundle.

        Needed once per draft when assets were added with ``finalize_files`` disabled. The bundle
        takes no further uploads afterwards. This does not commit the object — the draft stays open.
        """
        library_id = library_id or self.client.content_object_library_id(write_token=write_token)
        self.client.finalize_files(write_token=write_token, library_id=library_id)
        logger.info(f"Sealed the file bundle of {write_token}")

        return WriteTokenInfo(
            object_id=self.client.content_object(write_token=write_token).get("id", ""),
            library_id=library_id,
            write_token=write_token
        )

    def _resolve_entity(
        self,
        write_token: str,
        object_id: str,
        entity_input
    ) -> tuple:
        """Return (entity_id, existing entity metadata or None for a new entity)."""
        if not entity_input.entity_id:
            if not entity_input.label:
                raise BadRequestException(
                    "A label is required to create a new ground truth entity "
                    "(pass an entity id instead to add to an existing one)")
            return _new_id(), None

        existing = self._read(
            f"{ENTITIES}/{entity_input.entity_id}", write_token=write_token)
        if existing is None:
            raise BadRequestException(
                f"Entity {entity_input.entity_id} not found in ground truth pool {object_id}. "
                f"A draft only sees the object's committed state plus its own writes, so an "
                f"entity created in another draft is invisible here until that draft is finalized.")

        return entity_input.entity_id, existing

    def _upload_assets(
        self,
        write_token: str,
        library_id: str,
        request: AddAssetsRequest
    ) -> List[Dict[str, Any]]:
        """Upload the request's files into the draft and build their sample_files entries."""
        if not request.assets:
            return []

        directory = request.file_path.strip("/")
        taken = self._existing_file_names(write_token, library_id, directory)

        file_jobs, sample_files = [], []
        for asset in request.assets:
            if not os.path.isfile(asset.local_path):
                raise BadRequestException(f"Asset file not found: {asset.local_path}")

            filename = _unique_name(_safe_filename(asset.filename), taken)
            taken.add(filename)
            out_path = f"{directory}/{filename}" if directory else filename

            file_jobs.append(ElvClient.FileJob(
                local_path=asset.local_path,
                out_path=out_path,
                mime_type=asset.mime_type or _guess_mime_type(filename)
            ))
            sample_files.append(_format_asset(asset, out_path))

        try:
            self.client.upload_files(
                write_token=write_token,
                library_id=library_id,
                file_jobs=file_jobs,
                finalize=request.finalize_files
            )
        except ValueError as exc:
            # upload_files turns a 409 on job creation into a ValueError. The usual cause is a
            # draft whose file bundle was sealed by an earlier upload ("item is already
            # finalized"), which no longer accepts file jobs.
            raise BadRequestException(
                f"Upload rejected for {write_token}: {exc}. Paths attempted: "
                f"{', '.join(job.out_path for job in file_jobs)}. Use a fresh write token, or "
                "upload to a shared draft with finalize_files disabled and seal it once at the "
                "end.") from exc

        if not request.finalize_files:
            logger.info(
                f"Left the file bundle of {write_token} open; seal it with finalize_files() "
                "before finalizing the object")

        return sample_files

    def _validate_attributes(self, write_token: str, meta: Optional[Dict[str, str]]) -> None:
        """Reject attributes the pool's entity_data_schema doesn't declare."""
        if not meta:
            return

        schema = self._read(f"{GROUND_TRUTH}/entity_data_schema", write_token=write_token) or {}
        attributes = _schema_attributes(schema)
        if not attributes:
            # Pool declares no schema, so any attribute is as good as another.
            return

        unknown = [key for key in meta if key not in attributes]
        if unknown:
            raise BadRequestException(
                f"Unknown entity attribute(s): {', '.join(sorted(unknown))}. "
                f"Pool schema declares: {', '.join(attributes)}")

    def _existing_file_names(self, write_token: str, library_id: str, directory: str) -> Set[str]:
        """Names already present in the draft's asset directory, used to avoid overwriting."""
        try:
            entries = self.client.list_files(
                library_id=library_id, write_token=write_token, path=directory or "/")
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return set()
            raise
        except (AttributeError, TypeError):
            # list_files walks into the listing by path; a directory that doesn't exist in the
            # draft yet leaves it with nothing to walk. Warn rather than stay silent: an empty
            # listing here means an upload can collide with a file that is already there.
            logger.warning(
                f"Could not list existing files under '{directory}' in {write_token}; "
                "uploads will not be checked for name collisions")
            return set()

        return {entry.rstrip("/") for entry in entries}

    def _read(
        self,
        subtree: str,
        object_id: Optional[str] = None,
        write_token: Optional[str] = None
    ) -> Any:
        """Read a metadata subtree, treating an absent path as empty rather than fatal."""
        try:
            return self.client.content_object_metadata(
                object_id=object_id, write_token=write_token, metadata_subtree=subtree)
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise


def _format_asset(asset: AssetInput, out_path: str) -> Dict[str, Any]:
    """Build a sample_files entry, matching GroundTruthStore.FormatAsset."""
    now = _timestamp()
    return {
        "id": _new_id(),
        "link": {"/": f"./files/{out_path}"},
        "label": asset.label or os.path.basename(out_path),
        "description": asset.description,
        "source": asset.source,
        "added_at": now,
        "updated_at": now
    }


def _strip_link_urls(metadata: Any, depth: int = 1) -> Any:
    """Reduce fabric links to their path.

    Reading metadata can produce a resolved ``url`` alongside a link's ``/``; that url is
    node- and token-specific and must never be written back (see StripFabricLinkUrls in
    elv-video-editor).
    """
    if depth > 10:
        return {}
    if isinstance(metadata, list):
        return [_strip_link_urls(item, depth + 1) for item in metadata]
    if isinstance(metadata, dict):
        if "/" in metadata:
            return {"/": metadata["/"]}
        return {key: _strip_link_urls(value, depth + 1) for key, value in metadata.items()}
    return metadata


def _schema_attributes(schema: Dict[str, Any]) -> List[str]:
    """Attribute keys of a pool's entity_data_schema, in display order."""
    properties = (schema or {}).get("properties") or {}
    return sorted(properties, key=lambda key: (properties[key] or {}).get("order", 0))


def _new_id() -> str:
    """B58 encoded UUID, matching the ids the video editor generates."""
    return base58.b58encode(uuid.uuid4().bytes).decode("utf-8")


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _safe_filename(filename: str) -> str:
    """Reduce a caller-supplied name to a single path segment."""
    filename = os.path.basename((filename or "").strip().replace("\\", "/"))
    if not filename or filename in (".", ".."):
        raise BadRequestException(f"Invalid asset filename: {filename}")
    return filename


def _unique_name(filename: str, taken: Set[str]) -> str:
    """Suffix the name until it no longer collides with a file already in the object."""
    if filename not in taken:
        return filename

    stem, dot, extension = filename.rpartition(".")
    base = stem if dot else filename
    suffix = f".{extension}" if dot else ""
    index = 1
    while f"{base}_{index}{suffix}" in taken:
        index += 1

    return f"{base}_{index}{suffix}"


def _guess_mime_type(filename: str) -> str:
    return mimetypes.guess_type(filename)[0] or DEFAULT_MIME_TYPE


def _default_commit_message(entity_label: str, num_assets: int) -> str:
    if num_assets:
        return f"Add {num_assets} asset(s) to ground truth entity '{entity_label}'"
    return f"Update ground truth entity '{entity_label}'"
