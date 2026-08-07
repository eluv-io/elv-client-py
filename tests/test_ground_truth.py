"""Unit tests for the ground truth write flow, against an in-memory stand-in for the fabric."""

from copy import deepcopy

import pytest
import requests

from src.elv_client import ElvClient
from src.ground_truth.errors import BadRequestException
from src.ground_truth.model import AddAssetsRequest, AssetInput, EntityInput
from src.ground_truth.service import GroundTruthService

POOL_ID = "iq__pool"
LIBRARY_ID = "ilib__library"
WRITE_TOKEN = "tqw__token"


class FakeClient:
    """Records fabric calls and models draft isolation.

    Each write token is a snapshot of the committed metadata plus that draft's own writes, so a
    draft never sees what another draft wrote. Nothing here commits: the committed state only
    changes if a test changes it directly.
    """

    FileJob = ElvClient.FileJob

    def __init__(self, metadata=None):
        self.committed = metadata or {}
        self.drafts = {}
        self.minted = []
        self.uploaded = []
        self.finalized_uploads = []
        self.writes = []
        self.commit_messages = []

    @property
    def tokens_created(self):
        return len(self.minted)

    def content_object_library_id(self, object_id=None, **_kwargs):
        return LIBRARY_ID

    def content_object(self, object_id=None, version_hash=None, write_token=None, library_id=None):
        return {"id": POOL_ID, "qlib_id": LIBRARY_ID}

    def get_write_token(self, object_id, library_id=None, options=None):
        write_token = f"{WRITE_TOKEN}_{len(self.minted)}"
        self.minted.append(write_token)
        self._draft(write_token)
        return write_token

    def content_object_metadata(self, object_id=None, write_token=None, metadata_subtree="", **_kwargs):
        source = self._draft(write_token)["metadata"] if write_token else self.committed
        for key in [key for key in metadata_subtree.strip("/").split("/") if key]:
            if not isinstance(source, dict) or key not in source:
                raise _not_found()
            source = source[key]
        return source

    def list_files(self, library_id=None, write_token=None, path="/", **_kwargs):
        directory = path.strip("/")
        prefix = f"{directory}/" if directory else ""
        names = [
            file_path[len(prefix):]
            for file_path in self._draft(write_token)["files"]
            if file_path.startswith(prefix)
        ]
        if not names:
            raise _not_found()
        return names

    def upload_files(self, write_token, library_id, file_jobs, finalize=True):
        draft = self._draft(write_token)
        if draft["sealed"]:
            # What the fabric answers with: "item is already finalized" at openWriteBundle
            raise ValueError("Provided write token has already been used to upload files")

        self.uploaded.extend(file_jobs)
        self.finalized_uploads.append(finalize)
        draft["files"].extend(job.out_path for job in file_jobs)
        draft["sealed"] = finalize

    def finalize_files(self, write_token, library_id):
        self._draft(write_token)["sealed"] = True
        self.finalized_uploads.append(True)

    def replace_metadata(self, write_token, metadata, library_id, metadata_subtree=None):
        self.writes.append((metadata_subtree, metadata))

        target = self._draft(write_token)["metadata"]
        keys = [key for key in (metadata_subtree or "").strip("/").split("/") if key]
        for key in keys[:-1]:
            target = target.setdefault(key, {})
        target[keys[-1]] = deepcopy(metadata)

    def set_commit_message(self, write_token, message, library_id):
        self.commit_messages.append(message)

    def _draft(self, write_token):
        """Branch a draft off the committed state the first time it is used.

        A new draft is unsealed: the fabric only seals a file bundle once something finalizes it.
        """
        return self.drafts.setdefault(
            write_token, {"metadata": deepcopy(self.committed), "sealed": False, "files": []})


def _not_found():
    response = requests.Response()
    response.status_code = 404
    return requests.exceptions.HTTPError(response=response)


@pytest.fixture
def image(tmp_path):
    path = tmp_path / "tom.jpg"
    path.write_bytes(b"not really a jpeg")
    return str(path)


def _pool_metadata(entities=None, properties=None):
    return {
        "public": {"name": "Celebrities"},
        "ground_truth": {
            "model_domain": "celebrity_detection",
            "entity_data_schema": {"type": "object", "properties": properties or {}},
            "entities": entities or {}
        }
    }


def _committed_entity(entity_id="entity1", label="Tom Hanks", sample_files=None):
    return {entity_id: {"id": entity_id, "label": label, "sample_files": sample_files or []}}


def test_add_assets_creates_entity_and_uploads_files(image):
    client = FakeClient(_pool_metadata(properties={"gender": {"type": "string"}}))
    service = GroundTruthService(client)

    result = service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(label="Tom Hanks", meta={"gender": "male"}),
        assets=[AssetInput(local_path=image, filename="tom.jpg", anchor=True)]
    ))

    assert result.entity_created
    assert result.write_token == client.minted[0]
    assert client.tokens_created == 1

    # File landed in the object, and the entity links to that same path
    assert [(job.out_path, job.mime_type) for job in client.uploaded] == [("assets/tom.jpg", "image/jpeg")]
    subtree, entity = client.writes[0]
    assert subtree == f"/ground_truth/entities/{result.entity_id}"
    assert entity["label"] == "Tom Hanks"
    assert entity["meta"] == {"gender": "male"}
    assert entity["sample_files"][0]["link"] == {"/": "./files/assets/tom.jpg"}
    assert entity["sample_files"][0]["label"] == "tom.jpg"
    assert entity["sample_files"][0]["anchor"] is True
    assert client.commit_messages == ["Add 1 asset(s) to ground truth entity 'Tom Hanks'"]


def test_add_assets_appends_to_existing_entity_and_moves_anchor(image):
    client = FakeClient(_pool_metadata(entities={
        "entity1": {
            "id": "entity1",
            "label": "Tom Hanks",
            "sample_files": [
                {"id": "asset1", "link": {"/": "./files/assets/old.jpg", "url": "https://node/old.jpg"},
                 "label": "old.jpg", "anchor": True}
            ]
        }
    }))
    service = GroundTruthService(client)

    result = service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(entity_id="entity1"),
        assets=[AssetInput(local_path=image, filename="tom.jpg", label="New", anchor=True)],
        write_token=WRITE_TOKEN
    ))

    assert not result.entity_created
    assert result.total_assets == 2
    # Caller supplied the draft, so none was created
    assert client.tokens_created == 0

    _, entity = client.writes[0]
    assert [asset.get("anchor") for asset in entity["sample_files"]] == [None, True]
    # The resolved link url must not be written back
    assert entity["sample_files"][0]["link"] == {"/": "./files/assets/old.jpg"}


def test_upload_bundle_is_left_open_when_batching(image):
    """A shared draft only takes more uploads while its file bundle is unsealed."""
    client = FakeClient(_pool_metadata())
    service = GroundTruthService(client)

    service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(label="Tom Hanks"),
        assets=[AssetInput(local_path=image, filename="tom.jpg")],
        finalize_files=False
    ))
    assert client.finalized_uploads == [False]

    service.finalize_files(WRITE_TOKEN, LIBRARY_ID)
    assert client.finalized_uploads == [False, True]


def test_sealed_draft_takes_metadata_but_not_files(image):
    """Sealing closes the file bundle only; the draft still accepts metadata writes."""
    client = FakeClient(_pool_metadata(entities=_committed_entity()))
    service = GroundTruthService(client)

    def add(entity=None, **kwargs):
        return service.add_assets(AddAssetsRequest(
            object_id=POOL_ID,
            entity=EntityInput(entity_id="entity1", **(entity or {})),
            write_token=WRITE_TOKEN,
            **kwargs
        ))

    add(assets=[AssetInput(local_path=image, filename="tom.jpg")])
    assert client.drafts[WRITE_TOKEN]["sealed"]

    # Metadata-only writes still go through on the sealed draft
    result = add(entity={"description": "Actor", "label": "Tom Hanks Jr"})
    assert result.total_assets == 1
    _, entity = client.writes[-1]
    assert entity["description"] == "Actor"
    assert entity["label"] == "Tom Hanks Jr"

    writes_before = len(client.writes)
    with pytest.raises(BadRequestException, match="Upload rejected for"):
        add(assets=[AssetInput(local_path=image, filename="tom2.jpg")])

    # A rejected upload leaves the entity untouched rather than linking a file that is not there
    assert len(client.writes) == writes_before


def test_new_draft_cannot_reach_an_entity_left_in_an_uncommitted_draft(image):
    """A draft sees committed state plus its own writes, so an entity created in another draft
    and never committed is unreachable — for files and for metadata alike."""
    client = FakeClient(_pool_metadata())
    service = GroundTruthService(client)

    created = service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(label="Tom Hanks"),
        assets=[AssetInput(local_path=image, filename="tom.jpg")]
    ))

    # Visible in the draft that created it...
    assert client.content_object_metadata(
        write_token=created.write_token,
        metadata_subtree=f"ground_truth/entities/{created.entity_id}"
    )["label"] == "Tom Hanks"

    # ...but not in the committed object, so a new draft branches off without it
    with pytest.raises(BadRequestException, match="not found"):
        service.add_assets(AddAssetsRequest(
            object_id=POOL_ID,
            entity=EntityInput(entity_id=created.entity_id),
            assets=[AssetInput(local_path=image, filename="tom2.jpg")]
        ))

    with pytest.raises(BadRequestException, match="not found"):
        service.add_assets(AddAssetsRequest(
            object_id=POOL_ID,
            entity=EntityInput(entity_id=created.entity_id, description="Actor")
        ))

    assert client.tokens_created == 3


def test_new_draft_can_write_to_a_committed_entity(image):
    """The same writes succeed once the entity is part of the object's committed state."""
    client = FakeClient(_pool_metadata(entities=_committed_entity()))
    service = GroundTruthService(client)

    with_files = service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(entity_id="entity1"),
        assets=[AssetInput(local_path=image, filename="tom.jpg")]
    ))
    metadata_only = service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(entity_id="entity1", description="Actor")
    ))

    assert not with_files.entity_created and not metadata_only.entity_created
    assert with_files.write_token != metadata_only.write_token

    # Each draft branched off the committed entity, so neither one sees the other's asset
    assert with_files.total_assets == 1
    assert metadata_only.total_assets == 0


def test_add_assets_writes_metadata_without_files():
    client = FakeClient(_pool_metadata(entities=_committed_entity()))
    service = GroundTruthService(client)

    result = service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(entity_id="entity1", description="Actor")
    ))

    assert client.uploaded == []
    assert result.assets == []
    _, entity = client.writes[0]
    assert entity["description"] == "Actor"
    assert client.commit_messages == ["Update ground truth entity 'Tom Hanks'"]


def test_filename_collisions_do_not_overwrite(image):
    """Uploading to an unsealed draft a second time works, and does not overwrite what is there."""
    client = FakeClient(_pool_metadata(entities=_committed_entity()))
    service = GroundTruthService(client)

    service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(entity_id="entity1"),
        assets=[AssetInput(local_path=image, filename="tom.jpg")],
        write_token=WRITE_TOKEN,
        finalize_files=False
    ))
    service.add_assets(AddAssetsRequest(
        object_id=POOL_ID,
        entity=EntityInput(entity_id="entity1"),
        assets=[
            AssetInput(local_path=image, filename="tom.jpg"),
            AssetInput(local_path=image, filename="tom.jpg")
        ],
        write_token=WRITE_TOKEN
    ))

    assert [job.out_path for job in client.uploaded] == [
        "assets/tom.jpg", "assets/tom_1.jpg", "assets/tom_2.jpg"
    ]


def test_rejects_empty_request_unknown_attributes_and_missing_entity(image):
    service = GroundTruthService(FakeClient(_pool_metadata(properties={"gender": {"type": "string"}})))

    with pytest.raises(BadRequestException, match="Nothing to write"):
        service.add_assets(AddAssetsRequest(object_id=POOL_ID, entity=EntityInput()))

    with pytest.raises(BadRequestException, match="Unknown entity attribute"):
        service.add_assets(AddAssetsRequest(
            object_id=POOL_ID,
            entity=EntityInput(label="Tom Hanks", meta={"height": "6ft"})
        ))

    with pytest.raises(BadRequestException, match="not found"):
        service.add_assets(AddAssetsRequest(
            object_id=POOL_ID,
            entity=EntityInput(entity_id="nope"),
            assets=[AssetInput(local_path=image, filename="tom.jpg")]
        ))

    with pytest.raises(BadRequestException, match="label is required"):
        service.add_assets(AddAssetsRequest(
            object_id=POOL_ID,
            entity=EntityInput(),
            assets=[AssetInput(local_path=image, filename="tom.jpg")]
        ))


def test_pool_info_summarizes_entities():
    client = FakeClient(_pool_metadata(
        entities={"entity1": {"label": "Tom Hanks", "sample_files": [{"id": "a"}, {"id": "b"}]}},
        properties={"role": {"order": 1}, "gender": {"order": 0}}
    ))

    info = GroundTruthService(client).pool_info(POOL_ID)

    assert info.name == "Celebrities"
    assert info.model_domain == "celebrity_detection"
    assert info.attributes == ["gender", "role"]
    assert [(entity.id, entity.label, entity.num_assets) for entity in info.entities] == [
        ("entity1", "Tom Hanks", 2)
    ]
