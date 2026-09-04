"""Generated vNext schema-provider loader; do not edit by hand.

Regenerate with ``python scripts/generate-vnext-schema-provider.py``.
This module and its binary resource have no verification-package dependency.
"""

from __future__ import annotations

from ._schema_artifact_codec import _load_pinned_schema_artifact_resource

_RESOURCE_NAME = "_generated_vnext_schema.bin"
_PICKLE_PROTOCOL = 5
_RAW_SIZE = 4212755
_RAW_SHA256 = "e98954c3af10bcedd83f491f70231ed76dc58774d09a630f9fd1cf5addd34df5"

ARTIFACT = _load_pinned_schema_artifact_resource(
    package=__package__,
    resource_name=_RESOURCE_NAME,
    pickle_protocol=_PICKLE_PROTOCOL,
    raw_size=_RAW_SIZE,
    raw_sha256=_RAW_SHA256,
)

del _load_pinned_schema_artifact_resource
