"""Executable capacity limits for intentionally recomposed catalog registries."""

from __future__ import annotations

__all__ = ["RECOMPOSED_REGISTRY_MAXIMUM_ROWS"]


# The hash-bound MariaDB 10.11.11 capacity receipt measures 50,000
# maximum-width rows of the widest affected registry, including every
# generated secondary index. Registration is serialized by the ingest fence,
# so writers can enforce this ceiling before a fresh insert.
RECOMPOSED_REGISTRY_MAXIMUM_ROWS = 50_000
