"""Data model for the ground truth write flow.

These types are the boundary between callers (CLI now, REST layer later) and
``ground_truth.service``, so nothing here depends on how the request arrived.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Directory inside the pool object's file bundle that uploaded assets land in.
DEFAULT_FILE_PATH = "assets"


@dataclass
class AssetInput:
    """One image file to upload and link into an entity's sample_files."""

    local_path: str
    filename: str
    label: str = ""
    description: str = ""
    source: str = ""
    # Marks this asset as the entity's representative image. At most one asset
    # per entity carries the anchor flag; setting it clears the previous anchor.
    anchor: bool = False
    mime_type: Optional[str] = None


@dataclass
class EntityInput:
    """The entity the assets belong to.

    Provide ``entity_id`` to append to an existing entity, or ``label`` to create a new one.
    """

    entity_id: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    # Attribute values keyed by the pool's entity_data_schema properties.
    meta: Optional[Dict[str, str]] = None


@dataclass
class AddAssetsRequest:
    object_id: str
    entity: EntityInput
    assets: List[AssetInput] = field(default_factory=list)
    file_path: str = DEFAULT_FILE_PATH
    commit_message: Optional[str] = None
    # Write to an existing draft instead of creating one, so several calls can share a draft.
    write_token: Optional[str] = None
    library_id: Optional[str] = None
    # Seal the draft's file upload bundle once these files are in. A sealed bundle takes no
    # further uploads, so set this False when more calls will upload to the same draft, and
    # seal it explicitly (GroundTruthService.finalize_files) before finalizing the object.
    finalize_files: bool = True


@dataclass
class AddedAsset:
    id: str
    path: str
    link: str
    label: str
    anchor: bool


@dataclass
class AddAssetsResult:
    object_id: str
    library_id: str
    write_token: str
    entity_id: str
    entity_created: bool
    entity_label: str
    assets: List[AddedAsset]
    total_assets: int
    commit_message: str


@dataclass
class WriteTokenInfo:
    object_id: str
    library_id: str
    write_token: str


@dataclass
class EntitySummary:
    id: str
    label: str
    num_assets: int


@dataclass
class PoolInfo:
    object_id: str
    library_id: str
    name: str
    description: str
    model_domain: str
    attributes: List[str]
    entity_data_schema: Dict[str, Any]
    entities: List[EntitySummary]
