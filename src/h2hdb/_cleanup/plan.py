"""Source-owned immutable cleanup statement specifications.

Only literal target declarations construct these plans. Durable registry text
never supplies identifiers, predicates, or authorization."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from h2hdb._cleanup.model import CleanupTargetKind


@dataclass(frozen=True, slots=True)
class _StaticDeleteSpec:
    table: str
    source: str
    primary_key: tuple[str, ...]
    delete_sql: tuple[str, ...]
    extra_predicate: str = "1 = 1"
    delete_parameter_indexes: tuple[tuple[int, ...], ...] | None = None
    delete_allowed_affected: tuple[frozenset[int], ...] | None = None
    batch_exact_primary_keys: bool = False
    batch_delete_keys: tuple[tuple[str, tuple[str, ...]], ...] | None = None
    owner_key: tuple[str, ...] | None = None
    canonical_dictionary_columns: tuple[str, str] | None = None

    def __post_init__(self) -> None:
        if self.canonical_dictionary_columns is not None:
            dictionaries = {
                "catalog_display_title_choices": (
                    (
                        "display_title_policy_id",
                        "source_title_sha256",
                        "source_gallery_name",
                    ),
                    ("source_title_sha256", "title_sha256"),
                ),
                "catalog_title_sorts": (
                    ("title_sort_policy_id", "title_sha256"),
                    ("title_sha256", "sort_title_sha256"),
                ),
            }
            if dictionaries.get(self.table) != (
                self.primary_key,
                self.canonical_dictionary_columns,
            ):
                raise RuntimeError("canonical dictionary selection metadata is invalid")
        for metadata in (
            self.delete_parameter_indexes,
            self.delete_allowed_affected,
        ):
            if metadata is not None and len(metadata) != len(self.delete_sql):
                raise RuntimeError(
                    "cleanup compound-delete metadata must cover every statement"
                )
        if self.batch_delete_keys is not None:
            if (
                not self.batch_exact_primary_keys
                or self.delete_parameter_indexes is None
                or self.delete_allowed_affected is None
                or len(self.batch_delete_keys) != len(self.delete_sql)
            ):
                raise RuntimeError(
                    "batched compound cleanup requires complete key metadata"
                )
            for (table, columns), statement, indexes, allowed in zip(
                self.batch_delete_keys,
                self.delete_sql,
                self.delete_parameter_indexes,
                self.delete_allowed_affected,
                strict=True,
            ):
                if (
                    not columns
                    or len(columns) != len(indexes)
                    or any(
                        index < 0 or index >= len(self.primary_key) for index in indexes
                    )
                    or statement != _delete_sql(table, columns)
                    or allowed not in (frozenset((1,)), frozenset((0, 1)))
                ):
                    raise RuntimeError(
                        "batched compound cleanup requires exact unique-key deletes"
                    )
        elif self.batch_exact_primary_keys and (
            self.delete_sql != (_delete_sql(self.table, self.primary_key),)
            or self.delete_parameter_indexes is not None
            or self.delete_allowed_affected is not None
        ):
            raise RuntimeError("batched cleanup requires one exact primary-key delete")


@dataclass(frozen=True, slots=True)
class _StaticTargetPlan:
    kind: CleanupTargetKind
    root_table: str
    root_key: tuple[str, ...]
    shard_column: str
    shard_width: int | None
    eligibility: str
    phases: dict[str, tuple[_StaticDeleteSpec, ...]]
    uses_cutoff: bool = False
    variable_width_shard: bool = False
    contiguous_integer_prefix: bool = False


def _identifier(value: str) -> str:
    if not value or any(not (part.isalnum() or part == "_") for part in value):
        raise RuntimeError("cleanup SQL identifiers must be static ASCII identifiers")
    return value


def _delete_sql(table: str, primary_key: Sequence[str]) -> str:
    safe_table = _identifier(table)
    columns = tuple(_identifier(column) for column in primary_key)
    return f"DELETE FROM {safe_table} WHERE " + " AND ".join(
        f"{column} = %s" for column in columns
    )


def _owned_spec(
    table: str,
    primary_key: tuple[str, ...],
    root_table: str,
    root_key: tuple[str, ...],
    owner_key: tuple[str, ...] | None = None,
    *,
    extra_predicate: str = "1 = 1",
    delete_sql: tuple[str, ...] | None = None,
    delete_parameter_indexes: tuple[tuple[int, ...], ...] | None = None,
    delete_allowed_affected: tuple[frozenset[int], ...] | None = None,
    batch_exact_primary_keys: bool = False,
) -> _StaticDeleteSpec:
    if owner_key is None:
        owner_key = root_key
    if len(owner_key) != len(root_key):
        raise RuntimeError("cleanup owner and root key arity differ")
    safe_table = _identifier(table)
    safe_root = _identifier(root_table)
    join = " AND ".join(
        f"r.{_identifier(root)} = c.{_identifier(owner)}"
        for root, owner in zip(root_key, owner_key, strict=True)
    )
    return _StaticDeleteSpec(
        table=safe_table,
        source=f"{safe_table} AS c JOIN {safe_root} AS r ON {join}",
        primary_key=tuple(_identifier(column) for column in primary_key),
        delete_sql=(
            (_delete_sql(safe_table, primary_key),)
            if delete_sql is None
            else delete_sql
        ),
        extra_predicate=extra_predicate,
        delete_parameter_indexes=delete_parameter_indexes,
        delete_allowed_affected=delete_allowed_affected,
        batch_exact_primary_keys=batch_exact_primary_keys,
        owner_key=owner_key,
    )


def _indirect_spec(
    table: str,
    primary_key: tuple[str, ...],
    source: str,
    *,
    extra_predicate: str = "1 = 1",
    delete_sql: tuple[str, ...] | None = None,
    delete_parameter_indexes: tuple[tuple[int, ...], ...] | None = None,
    delete_allowed_affected: tuple[frozenset[int], ...] | None = None,
    batch_exact_primary_keys: bool = False,
    batch_delete_keys: tuple[tuple[str, tuple[str, ...]], ...] | None = None,
    canonical_dictionary_columns: tuple[str, str] | None = None,
) -> _StaticDeleteSpec:
    safe_table = _identifier(table)
    return _StaticDeleteSpec(
        table=safe_table,
        source=source,
        primary_key=tuple(_identifier(column) for column in primary_key),
        delete_sql=(
            (_delete_sql(safe_table, primary_key),)
            if delete_sql is None
            else delete_sql
        ),
        extra_predicate=extra_predicate,
        delete_parameter_indexes=delete_parameter_indexes,
        delete_allowed_affected=delete_allowed_affected,
        batch_exact_primary_keys=batch_exact_primary_keys,
        batch_delete_keys=batch_delete_keys,
        canonical_dictionary_columns=canonical_dictionary_columns,
    )
