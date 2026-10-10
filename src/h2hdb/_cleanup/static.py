"""Bounded static keyset selection, exact deletion, and terminal absence checks."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import replace

from h2hdb._cleanup import keys as _keys
from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup import roots as _roots
from h2hdb._cleanup.model import (
    _MAX_BATCH_ROWS,
    CleanupCorruptionError,
    CleanupRetentionBlockedError,
    CleanupTargetKind,
    CleanupUnavailableError,
    _CleanupOperation,
    _Mutation,
    _Mutator,
    _StaticScalar,
)
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan
from h2hdb.vnext_domains import require_digest32, require_uuid16
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

# Amortize terminal absence checks without an unbounded SQL expression. These
# are work budgets, not optimal batch-size claims. Bind counts stay below a
# conservative 999-variable SQLite connection, including frozen-root predicates.
_MAX_TERMINAL_PROBE_SPECS = 8
_MAX_TERMINAL_PROBE_BINDS = 900

# An exact-key SQL page is smaller than the durable transaction row budget.
_MAX_STATIC_DELETE_KEYS = 64
_MAX_STATIC_DELETE_BINDS = 900


def _static_select_sql(
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    *,
    exact: bool,
    frozen_root_predicate: str,
    has_after: bool = False,
    eligibility: str | None = None,
) -> str:
    root_columns = tuple(f"r.{column}" for column in plan.root_key)
    primary_columns = tuple(f"c.{column}" for column in spec.primary_key)
    ordered = root_columns + primary_columns
    select = ", ".join(ordered)
    shard_sql = _keys._static_shard_sql(plan)
    eligible = plan.eligibility if eligibility is None else eligibility
    if exact:
        exact_root = " AND ".join(f"{column} = %s" for column in root_columns)
        exact_pk = " AND ".join(f"{column} = %s" for column in primary_columns)
        return (
            f"SELECT {select} FROM {spec.source} WHERE ({eligible}) "
            f"AND ({spec.extra_predicate}) AND ({shard_sql}) "
            f"AND ({frozen_root_predicate}) "
            f"AND {exact_root} AND {exact_pk}"
        )
    keyset_sql = ""
    if has_after:
        keyset_sql = f" AND ({_keys._keyset_predicate(ordered)})"
    return (
        f"SELECT {select} FROM {spec.source} WHERE ({eligible}) "
        f"AND ({spec.extra_predicate}) AND ({shard_sql}) "
        f"AND ({frozen_root_predicate}) "
        f"{keyset_sql} ORDER BY {select} LIMIT %s"
    )


def _analysis_owned_suffix(
    plan: _StaticTargetPlan, spec: _StaticDeleteSpec
) -> tuple[str, ...] | None:
    """Use the declared owner prefix, without changing durable cursor tuples."""

    if (
        plan.kind is not CleanupTargetKind.ANALYSIS_RUN
        or plan.root_key != ("analysis_id",)
        or spec.owner_key != ("analysis_id",)
        or spec.primary_key[:1] != spec.owner_key
    ):
        return None
    return tuple(f"c.{column}" for column in spec.primary_key[1:])


def _analysis_cursor_suffix(
    values: tuple[_StaticScalar, ...], spec: _StaticDeleteSpec
) -> tuple[_StaticScalar, ...]:
    if len(values) != 1 + len(spec.primary_key) or values[0] != values[1]:
        raise CleanupCorruptionError("analysis cleanup cursor owner prefix drifted")
    require_uuid16(values[0], field="analysis cleanup cursor root")
    return values[2:]


def _select_static_candidates(
    operation: _CleanupOperation,
    *,
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    after: tuple[_StaticScalar, ...] | None,
    eligibility: str | None,
    policy: tuple[object, ...],
    shard: tuple[object, ...],
    remaining: int,
) -> list[tuple[object, ...]]:
    if spec.canonical_dictionary_columns is not None:
        return _select_canonical_dictionary_candidates(
            operation,
            plan=plan,
            spec=spec,
            after=after,
            eligibility=eligibility,
            policy=policy,
            shard=shard,
            remaining=remaining,
        )
    suffix = _analysis_owned_suffix(plan, spec)
    if suffix is None:
        frozen, bindings = _roots._frozen_root_predicate(plan, operation.frozen_roots)
        query = _static_select_sql(
            plan,
            spec,
            exact=False,
            frozen_root_predicate=frozen,
            has_after=after is not None,
            eligibility=eligibility,
        )
        parameters = policy + shard + bindings
        if after is not None:
            parameters += _keys._keyset_parameters(after)
        return operation.work.connector.fetch_all(query, (*parameters, remaining))

    boundary = None if after is None else _analysis_cursor_suffix(after, spec)
    after_root = (
        None
        if after is None
        else require_uuid16(after[0], field="analysis cleanup cursor root")
    )
    roots = sorted(
        require_uuid16(root[0], field="analysis cleanup frozen root")
        for root in operation.frozen_roots
    )
    columns = ("r.analysis_id",) + tuple(f"c.{key}" for key in spec.primary_key)
    eligible = plan.eligibility if eligibility is None else eligibility
    selected: list[tuple[object, ...]] = []
    for root in roots:
        if after_root is not None and root < after_root:
            continue
        if after_root is not None and root == after_root and not suffix:
            continue
        frozen, bindings = _roots._frozen_root_predicate(plan, ((root,),))
        parameters = policy + shard + bindings
        keyset = ""
        if after_root is not None and root == after_root:
            assert boundary is not None
            keyset = f" AND ({_keys._keyset_predicate(suffix)})"
            parameters += _keys._keyset_parameters(boundary)
        ordering = " ORDER BY " + ", ".join(suffix) if suffix else ""
        query = (
            f"SELECT {', '.join(columns)} FROM {spec.source} "
            f"WHERE ({eligible}) AND ({spec.extra_predicate}) "
            f"AND ({_keys._static_shard_sql(plan)}) AND ({frozen})"
            f"{keyset}{ordering} LIMIT %s"
        )
        selected.extend(
            operation.work.connector.fetch_all(
                query, (*parameters, remaining - len(selected))
            )
        )
        if len(selected) == remaining:
            break
    return selected


def _select_canonical_dictionary_candidates(
    operation: _CleanupOperation,
    *,
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    after: tuple[_StaticScalar, ...] | None,
    eligibility: str | None,
    policy: tuple[object, ...],
    shard: tuple[object, ...],
    remaining: int,
) -> list[tuple[object, ...]]:
    """Admit roots once, then merge two bounded indexed dictionary pages.

    An OR join can scan the entire retained dictionary before returning an
    empty page. The two equality joins use its existing reverse indexes. Their
    root admission belongs only to this selection in the current transaction;
    exact locking, cursor-covered responsibility, and terminal checks retain
    their independent predicates. Each arm returns at most ``remaining`` rows,
    so their merged prefix needs at most twice the transaction's hard cap.
    """
    references = spec.canonical_dictionary_columns
    if (
        references is None
        or plan.kind is not CleanupTargetKind.CANONICAL_VALUE
        or plan.root_key != ("value_sha256",)
        or not 1 <= remaining <= _MAX_BATCH_ROWS
        or len(operation.frozen_roots) > _MAX_BATCH_ROWS
        or (after is not None and len(after) != 1 + len(spec.primary_key))
    ):
        raise CleanupCorruptionError("canonical dictionary selection shape is invalid")
    if not operation.frozen_roots:
        return []
    frozen, bindings = _roots._frozen_root_predicate(plan, operation.frozen_roots)
    probes = " OR ".join(
        f"EXISTS (SELECT 1 FROM {spec.table} present_child "
        f"WHERE present_child.{_plan._identifier(column)} = r.value_sha256)"
        for column in references
    )
    boundary = ""
    parameters: tuple[object, ...] = shard + bindings
    if after is not None:
        boundary = " AND r.value_sha256 >= %s"
        parameters += (
            require_digest32(after[0], field="canonical dictionary cursor root"),
        )
    eligible = plan.eligibility if eligibility is None else eligibility
    # CASE prevents expensive reachability from running for roots with no raw
    # dictionary reference. This standalone root query has no child fanout.
    query = (
        f"SELECT r.value_sha256 FROM {plan.root_table} r "
        f"WHERE ({_keys._static_shard_sql(plan)}) AND ({frozen}){boundary} "
        f"AND (CASE WHEN ({probes}) THEN ({eligible}) ELSE 0 END) "
        f"ORDER BY r.value_sha256 LIMIT {_MAX_BATCH_ROWS}"
    )
    roots = tuple(
        _keys._static_values(row)
        for row in operation.work.connector.fetch_all(query, parameters + policy)
    )
    frozen_membership = set(operation.frozen_roots)
    if (
        len(roots) > _MAX_BATCH_ROWS
        or len(set(roots)) != len(roots)
        or any(root not in frozen_membership for root in roots)
    ):
        raise CleanupCorruptionError("canonical dictionary root admission drifted")
    if not roots:
        return []
    admitted, admitted_parameters = _roots._frozen_root_predicate(plan, roots)
    selected: set[tuple[_StaticScalar, ...]] = set()
    for column in references:
        branch = replace(
            spec,
            source=(
                f"{spec.table} AS c JOIN {plan.root_table} AS r "
                f"ON r.value_sha256 = c.{_plan._identifier(column)}"
            ),
        )
        statement = _static_select_sql(
            plan,
            branch,
            exact=False,
            frozen_root_predicate=admitted,
            has_after=after is not None,
            eligibility="1 = 1",
        )
        branch_parameters: tuple[object, ...] = shard + admitted_parameters
        if after is not None:
            branch_parameters += _keys._keyset_parameters(after)
        rows = operation.work.connector.fetch_all(
            statement, (*branch_parameters, remaining)
        )
        if len(rows) > remaining:
            raise CleanupCorruptionError("canonical dictionary page exceeds its bound")
        selected.update(_keys._static_values(row) for row in rows)
    # These two registered dictionaries order fixed-position integer and binary
    # keys; Python tuple order agrees with both engines' SQL order. Keep the
    # root in the key: the same child may belong to two distinct frozen roots.
    result: list[tuple[object, ...]] = []
    result.extend(sorted(selected)[:remaining])
    return result


def _static_raw_responsibility_query(
    *,
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    frozen_root_predicate: str,
    frozen_root_parameters: tuple[_StaticScalar, ...],
    shard_parameters: tuple[object, ...],
    through: tuple[_StaticScalar, ...] | None = None,
) -> tuple[str, tuple[object, ...]]:
    suffix = _analysis_owned_suffix(plan, spec)
    if through is not None and suffix is not None:
        boundary = _analysis_cursor_suffix(through, spec)
        base = (
            f"SELECT 1 FROM {spec.source} WHERE ({spec.extra_predicate}) "
            f"AND ({_keys._static_shard_sql(plan)}) AND ({frozen_root_predicate})"
        )
        bindings: tuple[object, ...] = shard_parameters + frozen_root_parameters
        before = base + " AND r.analysis_id < %s LIMIT 1"
        current = base + " AND r.analysis_id = %s"
        current_bindings = bindings + (through[0],)
        if suffix:
            branches = []
            for index, column in enumerate(suffix):
                equal = " AND ".join(f"{prior} = %s" for prior in suffix[:index])
                comparison = "<=" if index == len(suffix) - 1 else "<"
                branches.append(
                    f"({equal + ' AND ' if equal else ''}{column} {comparison} %s)"
                )
            current += " AND (" + " OR ".join(branches) + ")"
            current_bindings += _keys._keyset_parameters(boundary)
        current += " LIMIT 1"
        return (
            f"SELECT 1 WHERE EXISTS ({before}) OR EXISTS ({current})",
            bindings + (through[0],) + current_bindings,
        )
    ordered = tuple(f"r.{column}" for column in plan.root_key) + tuple(
        f"c.{column}" for column in spec.primary_key
    )
    covered = ""
    parameters: tuple[object, ...] = shard_parameters + frozen_root_parameters
    if through is not None:
        if len(through) != len(ordered):
            raise CleanupCorruptionError(
                "cleanup raw responsibility cursor arity drifted"
            )
        covered = f" AND NOT ({_keys._keyset_predicate(ordered)})"
        parameters += _keys._keyset_parameters(through)
    return (
        f"SELECT 1 FROM {spec.source} WHERE ({spec.extra_predicate}) "
        f"AND ({_keys._static_shard_sql(plan)}) "
        f"AND ({frozen_root_predicate}){covered} LIMIT 1",
        parameters,
    )


def _static_raw_responsibility_exists(
    work: VNextUnitOfWork,
    *,
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    frozen_root_predicate: str,
    frozen_root_parameters: tuple[_StaticScalar, ...],
    shard_parameters: tuple[object, ...],
    through: tuple[_StaticScalar, ...] | None = None,
) -> bool:
    query, parameters = _static_raw_responsibility_query(
        plan=plan,
        spec=spec,
        frozen_root_predicate=frozen_root_predicate,
        frozen_root_parameters=frozen_root_parameters,
        shard_parameters=shard_parameters,
        through=through,
    )
    return _responsibility_query_exists(work, query, parameters)


def _responsibility_query_exists(
    work: VNextUnitOfWork,
    query: str,
    parameters: tuple[object, ...],
) -> bool:
    row = work.connector.fetch_one(query, parameters)
    if not row:
        return False
    if row != (1,):
        raise CleanupCorruptionError(
            "cleanup raw responsibility probe returned an invalid shape"
        )
    return True


def _static_terminal_probe_batches(
    *,
    plan: _StaticTargetPlan,
    specs: Sequence[_StaticDeleteSpec],
    frozen_root_predicate: str,
    frozen_root_parameters: tuple[_StaticScalar, ...],
    shard_parameters: tuple[object, ...],
) -> Iterator[tuple[str, tuple[object, ...]]]:
    """Preserve every raw responsibility while bounding each SQL statement."""

    probes: list[str] = []
    parameters: tuple[object, ...] = ()
    for spec in specs:
        query, bindings = _static_raw_responsibility_query(
            plan=plan,
            spec=spec,
            frozen_root_predicate=frozen_root_predicate,
            frozen_root_parameters=frozen_root_parameters,
            shard_parameters=shard_parameters,
        )
        if len(bindings) > _MAX_TERMINAL_PROBE_BINDS:
            raise CleanupCorruptionError(
                "one cleanup responsibility exceeds the terminal probe bind budget"
            )
        if probes and (
            len(probes) == _MAX_TERMINAL_PROBE_SPECS
            or len(parameters) + len(bindings) > _MAX_TERMINAL_PROBE_BINDS
        ):
            yield "SELECT 1 WHERE " + " OR ".join(probes), parameters
            probes = []
            parameters = ()
        probes.append(f"EXISTS ({query})")
        parameters += bindings
    if probes:
        yield "SELECT 1 WHERE " + " OR ".join(probes), parameters


def _validate_static_cursor_covered_postcondition(
    work: VNextUnitOfWork,
    *,
    plan: _StaticTargetPlan,
    phase: str,
    start_index: int,
    start_values: tuple[_StaticScalar, ...] | None,
    frozen_root_predicate: str,
    frozen_root_parameters: tuple[_StaticScalar, ...],
    shard_parameters: tuple[object, ...],
) -> None:
    """Reject reappearance in the current spec's durable keyset prefix."""

    current_specs = plan.phases[phase]
    if start_values is not None and _static_raw_responsibility_exists(
        work,
        plan=plan,
        spec=current_specs[start_index],
        frozen_root_predicate=frozen_root_predicate,
        frozen_root_parameters=frozen_root_parameters,
        shard_parameters=shard_parameters,
        through=start_values,
    ):
        raise CleanupCorruptionError(
            f"{plan.kind.value} cursor-covered cleanup row reappeared"
        )


def _require_static_terminal_responsibility_empty(
    operation: _CleanupOperation,
    *,
    plan: _StaticTargetPlan,
    phase: str,
    frozen_root_predicate: str,
    frozen_root_parameters: tuple[_StaticScalar, ...],
    shard_parameters: tuple[object, ...],
) -> None:
    phase_names = tuple(plan.phases)
    try:
        phase_index = phase_names.index(phase)
    except ValueError as error:
        raise CleanupCorruptionError(
            "cleanup static phase is outside its registered plan"
        ) from error
    unchecked_phases = tuple(
        checked_phase
        for checked_phase in phase_names[: phase_index + 1]
        if checked_phase not in operation.empty_static_phases
    )
    specs = tuple(
        spec
        for checked_phase in unchecked_phases
        for spec in plan.phases[checked_phase]
    )
    for query, parameters in _static_terminal_probe_batches(
        plan=plan,
        specs=specs,
        frozen_root_predicate=frozen_root_predicate,
        frozen_root_parameters=frozen_root_parameters,
        shard_parameters=shard_parameters,
    ):
        if _responsibility_query_exists(operation.work, query, parameters):
            raise CleanupRetentionBlockedError(
                f"{plan.kind.value} still owns rows hidden by a retention predicate"
            )
    operation.empty_static_phases.update(unchecked_phases)


def _static_delete_page_size(*, fixed_binds: int, key_arity: int) -> int:
    """Bound a lock grid independently of the enclosing cleanup transaction."""

    if fixed_binds < 0 or key_arity < 1:
        raise CleanupCorruptionError("cleanup batch query shape is invalid")
    # One additional bind bounds even a malformed join to N + 1 returned rows.
    capacity = (_MAX_STATIC_DELETE_BINDS - fixed_binds - 1) // key_arity
    if capacity < 1:
        raise CleanupCorruptionError("cleanup batch predicates exceed the bind budget")
    return min(_MAX_STATIC_DELETE_KEYS, capacity)


def _delete_static_key_page(
    work: VNextUnitOfWork,
    *,
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    phase: str,
    index: int,
    candidates: tuple[tuple[_StaticScalar, ...], ...],
    frozen_predicate: str,
    fixed_parameters: tuple[object, ...],
    eligibility: str | None,
) -> None:
    """Revalidate one exact set before bounded child-first unique-key deletes.

    The exclusive maintenance gate already excludes supported writers. The
    locking read still repeats every eligibility, shard, frozen-root and spec
    predicate; its complete result must equal the selected keys. A derived
    ordinal preserves result and logical lock-key order for variable-length
    keys; it does not assert an optimizer's physical lock acquisition order.
    Compound families explicitly register each unique key and its projection
    from the selected tuple. Every statement preserves the original family
    order, and the enclosing transaction owns the entire family plus checkpoint.
    """

    root_arity = len(plan.root_key)
    columns = tuple(f"r.{column}" for column in plan.root_key) + tuple(
        f"c.{column}" for column in spec.primary_key
    )
    maximum = _static_delete_page_size(
        fixed_binds=len(fixed_parameters), key_arity=len(columns)
    )
    if not spec.batch_exact_primary_keys or not 1 <= len(candidates) <= maximum:
        raise CleanupCorruptionError("cleanup exact-key page exceeds its contract")
    if any(len(candidate) != len(columns) for candidate in candidates):
        raise CleanupCorruptionError("cleanup batch key arity drifted")
    primary_keys = tuple(candidate[root_arity:] for candidate in candidates)
    if len(set(primary_keys)) != len(primary_keys):
        raise CleanupCorruptionError("cleanup exact-key page repeats a primary key")
    lock_keys = tuple(
        encode_lock_key("cleanup-static", plan.kind.value, phase, index, *candidate)
        for candidate in candidates
    )
    if tuple(sorted(set(lock_keys))) != lock_keys:
        raise CleanupCorruptionError("cleanup exact-key page is not in lock order")

    grid: list[str] = []
    grid_parameters: list[_StaticScalar] = []
    for ordinal, candidate in enumerate(candidates):
        expressions = [f"{ordinal} AS cleanup_order"]
        for position, value in enumerate(candidate):
            expression = (
                work.connector.binary_parameter_expression(len(value))
                if isinstance(value, bytes) and len(value) in {16, 32}
                else "%s"
            )
            expressions.append(f"{expression} AS cleanup_key_{position}")
            grid_parameters.append(value)
        grid.append("SELECT " + ", ".join(expressions))
    join = " AND ".join(
        f"{column} = requested.cleanup_key_{position}"
        for position, column in enumerate(columns)
    )
    eligible = plan.eligibility if eligibility is None else eligibility
    query = (
        f"SELECT {', '.join(columns)} FROM {spec.source} "
        f"JOIN ({' UNION ALL '.join(grid)}) AS requested ON {join} "
        f"WHERE ({eligible}) AND ({spec.extra_predicate}) "
        f"AND ({_keys._static_shard_sql(plan)}) AND ({frozen_predicate}) "
        "ORDER BY requested.cleanup_order LIMIT %s"
    )
    locked = work.lock_rows(
        LockRank.CHILD,
        lock_keys,
        query,
        (*grid_parameters, *fixed_parameters, len(candidates) + 1),
    )
    if tuple(_keys._static_values(row) for row in locked) != candidates:
        raise CleanupRetentionBlockedError(
            f"{plan.kind.value} cleanup batch changed or gained a retention root"
        )

    families = spec.batch_delete_keys or ((spec.table, spec.primary_key),)
    projected_pages: list[tuple[tuple[_StaticScalar, ...], ...]] = []
    for index, (_table, columns) in enumerate(families):
        indexes = (
            tuple(range(len(spec.primary_key)))
            if spec.delete_parameter_indexes is None
            else spec.delete_parameter_indexes[index]
        )
        projected = tuple(
            tuple(primary[position] for position in indexes) for primary in primary_keys
        )
        if (
            len(set(projected)) != len(projected)
            or len(columns) * len(projected) > _MAX_STATIC_DELETE_BINDS
        ):
            raise CleanupCorruptionError(
                "cleanup compound page repeats or exceeds unique keys"
            )
        projected_pages.append(projected)
    for index, ((table, columns), projected) in enumerate(
        zip(families, projected_pages, strict=True)
    ):
        predicate = " AND ".join(f"{column} = %s" for column in columns)
        statement = f"DELETE FROM {table} WHERE " + " OR ".join(
            f"({predicate})" for _ in projected
        )
        affected = work.connector.execute_affected(
            statement, tuple(value for primary in projected for value in primary)
        )
        allowed = (
            frozenset((1,))
            if spec.delete_allowed_affected is None
            else spec.delete_allowed_affected[index]
        )
        # Every registered key is unique and every projected key is distinct:
        # required children must all be removed, optional children may each
        # contribute zero or one row, exactly as in the original scalar path.
        if (
            not min(allowed) * len(projected)
            <= affected
            <= max(allowed) * len(projected)
        ):
            raise CleanupUnavailableError(f"{plan.kind.value} cleanup batch changed")


def _run_static_phase(
    operation: _CleanupOperation,
    cursor: bytes,
    plan: _StaticTargetPlan,
    phase: str,
    *,
    eligibility: str | None = None,
    policy_parameters: tuple[object, ...] | None = None,
) -> _Mutation:
    work, cycle = operation.work, operation.cycle
    specs = plan.phases[phase]
    frozen_roots = operation.frozen_roots
    start_index, start_values = _keys._decode_static_cursor(
        cursor, specs, len(plan.root_key)
    )
    frozen_predicate, frozen_parameters = _roots._frozen_root_predicate(
        plan, frozen_roots
    )
    deleted: list[bytes] = []
    next_cursor = cursor
    policy = (
        _keys._static_policy_parameters(plan, cycle)
        if policy_parameters is None
        else policy_parameters
    )
    shard = _keys._static_shard_parameters(plan, cycle)
    _validate_static_cursor_covered_postcondition(
        work,
        plan=plan,
        phase=phase,
        start_index=start_index,
        start_values=start_values,
        frozen_root_predicate=frozen_predicate,
        frozen_root_parameters=frozen_parameters,
        shard_parameters=shard,
    )
    continue_in_next_transaction = False
    for index in range(start_index, len(specs)):
        spec = specs[index]
        deleted_primary_keys: set[tuple[_StaticScalar, ...]] = set()
        ordered_arity = len(plan.root_key) + len(spec.primary_key)
        after = start_values if index == start_index else None
        if after is not None and len(after) != ordered_arity:
            raise CleanupCorruptionError("cleanup cursor does not match relation key")
        while len(deleted) < cycle.max_rows_per_transaction:
            remaining = cycle.max_rows_per_transaction - len(deleted)
            rows = _select_static_candidates(
                operation,
                plan=plan,
                spec=spec,
                after=after,
                eligibility=eligibility,
                policy=policy,
                shard=shard,
                remaining=remaining,
            )
            if not rows:
                break
            candidates = tuple(_keys._static_values(row) for row in rows)
            if len(candidates) > remaining or (
                after is not None and candidates[-1] <= after
            ):
                raise CleanupCorruptionError("cleanup candidate page did not advance")
            deleted_before_page = len(deleted)
            candidates_by_lock = tuple(
                sorted(
                    candidates,
                    key=lambda candidate: encode_lock_key(
                        "cleanup-static",
                        plan.kind.value,
                        phase,
                        index,
                        *candidate,
                    ),
                )
            )
            if spec.batch_exact_primary_keys:
                fixed_parameters = policy + shard + frozen_parameters
                page_size = _static_delete_page_size(
                    fixed_binds=len(fixed_parameters), key_arity=ordered_arity
                )
                unique_candidates: list[tuple[_StaticScalar, ...]] = []
                for candidate in candidates_by_lock:
                    primary = candidate[len(plan.root_key) :]
                    if primary not in deleted_primary_keys:
                        unique_candidates.append(candidate)
                        deleted_primary_keys.add(primary)
                for start in range(0, len(unique_candidates), page_size):
                    page = tuple(unique_candidates[start : start + page_size])
                    _delete_static_key_page(
                        work,
                        plan=plan,
                        spec=spec,
                        phase=phase,
                        index=index,
                        candidates=page,
                        frozen_predicate=frozen_predicate,
                        fixed_parameters=fixed_parameters,
                        eligibility=eligibility,
                    )
                    deleted.extend(
                        _keys._encode_static_cursor(index, key) for key in page
                    )
                if len(deleted) == deleted_before_page:
                    raise CleanupCorruptionError(
                        "cleanup candidate page made no progress"
                    )
                after = candidates[-1]
                next_cursor = _keys._encode_static_cursor(index, after)
                if len(candidates) == remaining and len(
                    deleted
                ) - deleted_before_page < len(candidates):
                    # Keep this spec's cursor for the next transaction. SQL
                    # tuple order and encoded lock-key order can differ for
                    # variable-width keys, so do not lock another joined page.
                    continue_in_next_transaction = True
                    break
                if len(candidates) < remaining:
                    break
                continue
            for candidate in candidates_by_lock:
                root = candidate[: len(plan.root_key)]
                primary = candidate[len(plan.root_key) :]
                if primary in deleted_primary_keys:
                    # An indirect child can reference two eligible roots (for
                    # example a display-title cache row whose input and output
                    # canonical digests share this shard).  Delete the child
                    # exactly once while still advancing past every joined
                    # root/key tuple in the deterministic cursor order.
                    continue
                exact_query = _static_select_sql(
                    plan,
                    spec,
                    exact=True,
                    frozen_root_predicate=frozen_predicate,
                    eligibility=eligibility,
                )
                exact_parameters = policy + shard + frozen_parameters + root + primary
                lock_key = encode_lock_key(
                    "cleanup-static",
                    plan.kind.value,
                    phase,
                    index,
                    *candidate,
                )
                locked = work.lock_row(
                    LockRank.CHILD,
                    lock_key,
                    exact_query,
                    exact_parameters,
                )
                if _keys._static_values(locked) != candidate:
                    raise CleanupRetentionBlockedError(
                        f"{plan.kind.value} gained a retention root"
                    )
                for statement_index, statement in enumerate(spec.delete_sql):
                    indexes = spec.delete_parameter_indexes
                    statement_parameters = (
                        primary
                        if indexes is None
                        else tuple(primary[index] for index in indexes[statement_index])
                    )
                    affected = work.connector.execute_affected(
                        statement,
                        statement_parameters,
                    )
                    allowed = spec.delete_allowed_affected
                    expected = (
                        frozenset((1,)) if allowed is None else allowed[statement_index]
                    )
                    if affected not in expected:
                        raise CleanupUnavailableError(
                            f"{plan.kind.value} cleanup row changed"
                        )
                deleted_primary_keys.add(primary)
                deleted.append(_keys._encode_static_cursor(index, candidate))
            # Cursor order is SQL root/PK order, independent of the unsigned
            # encoded lock-key order used above inside this bounded page.
            if len(deleted) == deleted_before_page:
                raise CleanupCorruptionError("cleanup candidate page made no progress")
            after = candidates[-1]
            next_cursor = _keys._encode_static_cursor(index, after)
            # A duplicate reference does not exhaust this spec. Continue from
            # its exact tuple in a new transaction, with fresh lock ordering.
            if len(candidates) == remaining and len(
                deleted
            ) - deleted_before_page < len(candidates):
                continue_in_next_transaction = True
                break
            if len(candidates) < remaining:
                break
        if continue_in_next_transaction:
            break
    if not deleted:
        _require_static_terminal_responsibility_empty(
            operation,
            plan=plan,
            phase=phase,
            frozen_root_predicate=frozen_predicate,
            frozen_root_parameters=frozen_parameters,
            shard_parameters=shard,
        )
    return _Mutation(next_cursor, tuple(deleted))


def _static_mutator(plan: _StaticTargetPlan, phase: str) -> _Mutator:
    def mutate(operation: _CleanupOperation, cursor: bytes) -> _Mutation:
        cycle = operation.cycle
        if cycle.target_kind != plan.kind:
            raise CleanupCorruptionError("cleanup static strategy kind drifted")
        return _run_static_phase(operation, cursor, plan, phase)

    return mutate
