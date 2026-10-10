"""Generated vNext schema-provider loader; do not edit by hand.

Regenerate with ``python scripts/generate-vnext-schema-provider.py``.
This module and its binary resource have no verification-package dependency.
"""

from __future__ import annotations

from ._schema_artifact_codec import _load_pinned_schema_artifact_resource

_RESOURCE_NAME = "_generated_vnext_schema.bin"
_PICKLE_PROTOCOL = 5
_RAW_SIZE = 4437047
_RAW_SHA256 = "a2e18b6b96a832142c7542a891cf07e9a6f5e38ec793428295a5b89e9f1c4d0e"

ARTIFACT = _load_pinned_schema_artifact_resource(
    package=__package__,
    resource_name=_RESOURCE_NAME,
    pickle_protocol=_PICKLE_PROTOCOL,
    raw_size=_RAW_SIZE,
    raw_sha256=_RAW_SHA256,
)

del _load_pinned_schema_artifact_resource
