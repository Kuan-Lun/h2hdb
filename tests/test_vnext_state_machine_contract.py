from __future__ import annotations

import ast
import re
import tomllib
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import catalog_writer
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.domain import CatalogResourceKind
from h2hdb.vnext_analysis_family import AnalysisRunFamily, cas_analysis_run_state
from h2hdb.vnext_artifact_family import cas_prepared_artifact_state
from h2hdb.vnext_manifest_family import SourceBuildFamily
from h2hdb.vnext_state_machine_contract import (
    CATALOG_STATE_MACHINE_GATE_RELATIONS,
    CATALOG_STATE_MACHINE_TRANSITION_GATES,
    CATALOG_STATE_MACHINE_WRITER_ENTRYPOINTS,
    CATALOG_STATE_MACHINES,
    CATALOG_STATE_MUTATION_SITES,
    CatalogStateMachineContractError,
    catalog_transition_is_valid,
    require_catalog_state_mutation,
    require_catalog_transition,
    validate_catalog_state_machine_contract,
)

ROOT = Path(__file__).resolve().parents[1]
_DML = re.compile(r"\s*(INSERT\s+INTO|UPDATE)\s+([A-Za-z0-9_]+)", re.IGNORECASE)
_EXECUTE_METHODS = frozenset({"execute", "execute_affected", "compare_and_swap"})
type MutationSiteIdentity = tuple[str, str, str, str]


@dataclass(frozen=True, slots=True)
class _UnresolvedValue:
    pass


_UNRESOLVED = _UnresolvedValue()
type StaticValue = str | None | _UnresolvedValue


@dataclass(frozen=True, slots=True)
class _MutationObservation:
    identity: MutationSiteIdentity
    query: str | None
    parameters: ast.expr | None
    lineno: int


@dataclass(frozen=True, slots=True)
class _TransitionBinding:
    module: str
    function: str
    site_id: str
    target: str | None
    keyword_names: frozenset[str]
    previous_state: StaticValue
    next_state: StaticValue
    timestamp: StaticValue
    lineno: int


@dataclass(frozen=True, slots=True)
class _CallObservation:
    module: str
    function: str
    name: str
    keywords: tuple[tuple[str, ast.expr], ...]
    lineno: int


def _literal(expression: ast.expr, constants: Mapping[str, str]) -> str | None:
    if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
        return expression.value
    if isinstance(expression, ast.Name):
        return constants.get(expression.id)
    if isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Add):
        left = _literal(expression.left, constants)
        right = _literal(expression.right, constants)
        return None if left is None or right is None else left + right
    if isinstance(expression, ast.JoinedStr):
        parts: list[str] = []
        for value in expression.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
                continue
            if (
                isinstance(value, ast.FormattedValue)
                and isinstance(value.value, ast.Name)
                and value.conversion == -1
                and value.format_spec is None
            ):
                resolved = constants.get(value.value.id)
                if resolved is not None:
                    parts.append(resolved)
                    continue
            return None
        return "".join(parts)
    return None


def _module_constants(tree: ast.Module) -> dict[str, str]:
    result: dict[str, str] = {}
    for statement in tree.body:
        target: ast.expr | None = None
        value: ast.expr | None = None
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
            value = statement.value
        elif isinstance(statement, ast.AnnAssign):
            target = statement.target
            value = statement.value
        if isinstance(target, ast.Name) and value is not None:
            resolved = _literal(value, result)
            if resolved is not None:
                result[target.id] = resolved
    return result


def _call_name(function: ast.expr) -> str | None:
    if isinstance(function, ast.Name):
        return function.id
    if isinstance(function, ast.Attribute):
        return function.attr
    return None


def _static_value(
    expression: ast.expr | None,
    constants: Mapping[str, str],
) -> StaticValue:
    if expression is None:
        return _UNRESOLVED
    if isinstance(expression, ast.Constant) and expression.value is None:
        return None
    value = _literal(expression, constants)
    return _UNRESOLVED if value is None else value


def _keyword_expression(call: ast.Call, name: str) -> ast.expr | None:
    return next(
        (keyword.value for keyword in call.keywords if keyword.arg == name),
        None,
    )


class _MutationVisitor(ast.NodeVisitor):
    def __init__(
        self,
        *,
        module: str,
        module_constants: Mapping[str, str],
        authority_tables: frozenset[str],
        timestamp_sink_calls: frozenset[str],
    ) -> None:
        self._module = module
        self._constants = dict(module_constants)
        self._authority_tables = authority_tables
        self._timestamp_sink_calls = timestamp_sink_calls
        self._scopes: list[str] = []
        self._execute_alias_scopes: list[dict[str, str | None]] = [{}]
        self._assigned_helper_targets: dict[int, str] = {}
        self.mutations: list[_MutationObservation] = []
        self.bindings: list[_TransitionBinding] = []
        self.calls: list[_CallObservation] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scopes.append(node.name)
        self._execute_alias_scopes.append({})
        self.generic_visit(node)
        self._execute_alias_scopes.pop()
        self._scopes.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._scopes.append(node.name)
        self._execute_alias_scopes.append(self._argument_shadows(node.args))
        self.generic_visit(node)
        self._execute_alias_scopes.pop()
        self._scopes.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._scopes.append(node.name)
        self._execute_alias_scopes.append(self._argument_shadows(node.args))
        self.generic_visit(node)
        self._execute_alias_scopes.pop()
        self._scopes.pop()

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            self._register_assigned_helpers(node.value, node.targets[0].id)
        for target in node.targets:
            self._bind_execute_alias(target, node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and node.value is not None:
            self._register_assigned_helpers(node.value, node.target.id)
        self._bind_execute_alias(node.target, node.value)
        self.generic_visit(node)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self._bind_execute_alias(node.target, node.value)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        name = _call_name(node.func)
        if name in self._timestamp_sink_calls:
            assert name is not None
            self.calls.append(
                _CallObservation(
                    self._module,
                    ".".join(self._scopes),
                    name,
                    tuple(
                        (keyword.arg, keyword.value)
                        for keyword in node.keywords
                        if keyword.arg is not None
                    ),
                    node.lineno,
                )
            )
        if name == "require_catalog_state_mutation":
            self._add_binding(node)
        query = (
            _literal(node.args[0], self._constants)
            if node.args
            else next(
                (
                    _literal(keyword.value, self._constants)
                    for keyword in node.keywords
                    if keyword.arg in {"sql", "statement", "query"}
                ),
                None,
            )
        )
        parameters = (
            node.args[1]
            if len(node.args) >= 2
            else next(
                (
                    keyword.value
                    for keyword in node.keywords
                    if keyword.arg in {"parameters", "params"}
                ),
                None,
            )
        )
        execute_method, through_alias = self._execute_call(node.func)
        if execute_method is not None:
            if through_alias and query is None:
                raise AssertionError(
                    "catalog lifecycle execute alias uses SQL that cannot be "
                    "resolved statically"
                )
        if query is not None:
            match = _DML.match(query)
            if match is not None and match.group(2) in self._authority_tables:
                if execute_method is None:
                    raise AssertionError(
                        "catalog lifecycle authority DML uses an unsupported call shape"
                    )
                operation = (
                    "INSERT"
                    if match.group(1).upper().startswith("INSERT")
                    else "UPDATE"
                )
                self._add_mutation(
                    match.group(2),
                    operation,
                    query=query,
                    parameters=parameters,
                    lineno=node.lineno,
                )
        if name == "_insert_or_compare" and len(node.args) >= 2:
            table = _literal(node.args[1], self._constants)
            if table in self._authority_tables:
                assert table is not None
                self._add_mutation(
                    table,
                    "INSERT",
                    query=None,
                    parameters=node.args[3] if len(node.args) >= 4 else None,
                    lineno=node.lineno,
                )
        self.generic_visit(node)

    @staticmethod
    def _argument_shadows(arguments: ast.arguments) -> dict[str, str | None]:
        names = {
            argument.arg
            for argument in (
                *arguments.posonlyargs,
                *arguments.args,
                *arguments.kwonlyargs,
            )
        }
        if arguments.vararg is not None:
            names.add(arguments.vararg.arg)
        if arguments.kwarg is not None:
            names.add(arguments.kwarg.arg)
        return dict.fromkeys(names)

    def _execute_call(self, expression: ast.expr) -> tuple[str | None, bool]:
        name = _call_name(expression)
        if isinstance(expression, ast.Attribute):
            return (name, False) if name in _EXECUTE_METHODS else (None, False)
        if isinstance(expression, ast.Name):
            for scope in reversed(self._execute_alias_scopes):
                if expression.id in scope:
                    method = scope[expression.id]
                    return method, method is not None
            if name in _EXECUTE_METHODS:
                return name, True
        return None, False

    def _execute_method(self, expression: ast.expr) -> str | None:
        return self._execute_call(expression)[0]

    def _bind_execute_alias(
        self,
        target: ast.expr,
        value: ast.expr | None,
    ) -> None:
        if isinstance(target, ast.Name):
            self._execute_alias_scopes[-1][target.id] = (
                None if value is None else self._execute_method(value)
            )
            return
        for name in (
            child.id for child in ast.walk(target) if isinstance(child, ast.Name)
        ):
            self._execute_alias_scopes[-1][name] = None

    def _register_assigned_helpers(self, expression: ast.expr, target: str) -> None:
        for value in ast.walk(expression):
            if (
                isinstance(value, ast.Call)
                and _call_name(value.func) == "require_catalog_state_mutation"
            ):
                self._assigned_helper_targets[id(value)] = target

    def _add_binding(self, call: ast.Call) -> None:
        site_expression = (
            call.args[0] if call.args else _keyword_expression(call, "site_id")
        )
        site_id = (
            _literal(site_expression, self._constants) if site_expression else None
        )
        self.bindings.append(
            _TransitionBinding(
                self._module,
                ".".join(self._scopes),
                "<dynamic>" if site_id is None else site_id,
                self._assigned_helper_targets.get(id(call)),
                frozenset(
                    keyword.arg for keyword in call.keywords if keyword.arg is not None
                ),
                _static_value(
                    _keyword_expression(call, "previous_state"),
                    self._constants,
                ),
                _static_value(
                    _keyword_expression(call, "next_state"),
                    self._constants,
                ),
                _static_value(
                    _keyword_expression(call, "timestamp"),
                    self._constants,
                ),
                call.lineno,
            )
        )

    def _add_mutation(
        self,
        table: str,
        operation: str,
        *,
        query: str | None,
        parameters: ast.expr | None,
        lineno: int,
    ) -> None:
        self.mutations.append(
            _MutationObservation(
                (self._module, ".".join(self._scopes), table, operation),
                query,
                parameters,
                lineno,
            )
        )


def _inspect_catalog_state_mutations(
    source_overrides: Mapping[str, str] | None = None,
) -> tuple[
    tuple[_MutationObservation, ...],
    tuple[_TransitionBinding, ...],
    tuple[_CallObservation, ...],
]:
    sinks = tuple(
        site.timestamp_sink
        for site in CATALOG_STATE_MUTATION_SITES
        if site.timestamp_sink is not None
    )
    authority_tables = frozenset(
        (
            *(site.table for site in CATALOG_STATE_MUTATION_SITES),
            *(sink.table for sink in sinks),
        )
    )
    timestamp_sink_calls = frozenset(sink.function for sink in sinks)
    mutations: list[_MutationObservation] = []
    bindings: list[_TransitionBinding] = []
    calls: list[_CallObservation] = []
    overrides = {} if source_overrides is None else source_overrides
    for path in sorted((ROOT / "src" / "h2hdb").glob("*.py")):
        module = f"h2hdb.{path.stem}"
        source = overrides.get(module, path.read_text(encoding="utf-8"))
        tree = ast.parse(source, filename=str(path))
        visitor = _MutationVisitor(
            module=module,
            module_constants=_module_constants(tree),
            authority_tables=authority_tables,
            timestamp_sink_calls=timestamp_sink_calls,
        )
        visitor.visit(tree)
        mutations.extend(visitor.mutations)
        bindings.extend(visitor.bindings)
        calls.extend(visitor.calls)
    return tuple(mutations), tuple(bindings), tuple(calls)


def _sql_csv_parts(value: str, *, offset: int) -> tuple[tuple[str, int], ...]:
    parts: list[tuple[str, int]] = []
    cursor = 0
    for raw in value.split(","):
        leading = len(raw) - len(raw.lstrip())
        parts.append((raw.strip(), offset + cursor + leading))
        cursor += len(raw) + 1
    return tuple(parts)


def _insert_parameter_index(query: str, column: str) -> int | None:
    dml = _DML.match(query)
    if dml is None or not dml.group(1).upper().startswith("INSERT"):
        return None
    columns_open = query.find("(", dml.end())
    columns_close = query.find(")", columns_open + 1)
    if columns_open < 0 or columns_close < 0:
        return None
    columns = tuple(
        part.strip().strip("`")
        for part in query[columns_open + 1 : columns_close].split(",")
    )
    if column not in columns:
        return None
    value_index = columns.index(column)
    suffix = query[columns_close + 1 :]
    values_match = re.search(r"\bVALUES\s*\(", suffix, re.IGNORECASE)
    if values_match is not None:
        expressions_start = columns_close + 1 + values_match.end()
        expressions_close = query.find(")", expressions_start)
        if expressions_close < 0:
            return None
        expressions = _sql_csv_parts(
            query[expressions_start:expressions_close],
            offset=expressions_start,
        )
    else:
        select_match = re.search(r"\bSELECT\s+", suffix, re.IGNORECASE)
        if select_match is None:
            return None
        expressions_start = columns_close + 1 + select_match.end()
        from_match = re.search(
            r"\s+FROM\s+",
            query[expressions_start:],
            re.IGNORECASE,
        )
        if from_match is None:
            return None
        expressions_close = expressions_start + from_match.start()
        expressions = _sql_csv_parts(
            query[expressions_start:expressions_close],
            offset=expressions_start,
        )
    if len(expressions) != len(columns):
        return None
    expression, position = expressions[value_index]
    if expression != "%s":
        return None
    return query[:position].count("%s")


def _update_parameter_index(
    query: str,
    column: str,
    *,
    where: bool,
) -> int | None:
    dml = _DML.match(query)
    if dml is None or dml.group(1).upper() != "UPDATE":
        return None
    where_match = re.search(r"\bWHERE\b", query, re.IGNORECASE)
    if where_match is None:
        return None
    start, stop = (
        (where_match.end(), len(query)) if where else (dml.end(), where_match.start())
    )
    clause = query[start:stop]
    matches = tuple(
        re.finditer(
            rf"(?:\b|`){re.escape(column)}(?:\b|`)\s*=\s*(%s)",
            clause,
            re.IGNORECASE,
        )
    )
    if len(matches) != 1:
        return None
    position = start + matches[0].start(1)
    return query[:position].count("%s")


def _assignment_parameter_index(
    query: str | None,
    operation: str,
    column: str,
) -> int | None:
    if query is None:
        return None
    if operation == "INSERT":
        return _insert_parameter_index(query, column)
    return _update_parameter_index(query, column, where=False)


def _assignment_mentions_column(
    query: str | None,
    operation: str,
    column: str,
) -> bool:
    if query is None:
        return False
    if operation == "UPDATE":
        where_match = re.search(r"\bWHERE\b", query, re.IGNORECASE)
        assignment = query if where_match is None else query[: where_match.start()]
        return (
            re.search(
                rf"\b{re.escape(column)}\b\s*=",
                assignment,
                re.IGNORECASE,
            )
            is not None
        )
    dml = _DML.match(query)
    if dml is None:
        return False
    opening = query.find("(", dml.end())
    closing = query.find(")", opening + 1)
    if opening < 0 or closing < 0:
        return False
    columns = {
        value.strip().strip("`") for value in query[opening + 1 : closing].split(",")
    }
    return column in columns


def _parameter_is_attribute(
    parameters: ast.expr | None,
    index: int,
    target: str,
    attribute: str,
) -> bool:
    if not isinstance(parameters, (ast.List, ast.Tuple)):
        return False
    if index >= len(parameters.elts) or any(
        isinstance(element, ast.Starred) for element in parameters.elts
    ):
        return False
    expression = parameters.elts[index]
    return (
        isinstance(expression, ast.Attribute)
        and isinstance(expression.value, ast.Name)
        and expression.value.id == target
        and expression.attr == attribute
    )


def _parameter_is_name(
    parameters: ast.expr | None,
    index: int,
    name: str,
) -> bool:
    if not isinstance(parameters, (ast.List, ast.Tuple)):
        return False
    if index >= len(parameters.elts) or any(
        isinstance(element, ast.Starred) for element in parameters.elts
    ):
        return False
    expression = parameters.elts[index]
    return isinstance(expression, ast.Name) and expression.id == name


def _call_keyword(call: _CallObservation, name: str) -> ast.expr | None:
    return next(
        (expression for keyword, expression in call.keywords if keyword == name),
        None,
    )


def _is_required_timestamp_forward(expression: ast.expr | None, target: str) -> bool:
    if not isinstance(expression, ast.IfExp):
        return False
    if not (
        isinstance(expression.body, ast.Attribute)
        and isinstance(expression.body.value, ast.Name)
        and expression.body.value.id == target
        and expression.body.attr == "required_timestamp"
    ):
        return False
    test = expression.test
    return (
        isinstance(test, ast.Compare)
        and isinstance(test.left, ast.Name)
        and test.left.id == target
        and len(test.ops) == 1
        and isinstance(test.ops[0], ast.IsNot)
        and len(test.comparators) == 1
        and isinstance(test.comparators[0], ast.Constant)
        and test.comparators[0].value is None
    )


def _require_source_contract(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _validate_catalog_state_mutation_sources(
    source_overrides: Mapping[str, str] | None = None,
) -> None:
    mutations, bindings, calls = _inspect_catalog_state_mutations(source_overrides)
    sites_by_id = {site.site_id: site for site in CATALOG_STATE_MUTATION_SITES}
    machines = {machine.name: machine for machine in CATALOG_STATE_MACHINES}
    expected_mutations = Counter(
        (site.module, site.function, site.table, site.operation)
        for site in CATALOG_STATE_MUTATION_SITES
    )
    expected_mutations.update(
        (
            site.module,
            site.timestamp_sink.function,
            site.timestamp_sink.table,
            site.timestamp_sink.operation,
        )
        for site in CATALOG_STATE_MUTATION_SITES
        if site.timestamp_sink is not None
    )
    actual_mutations = Counter(mutation.identity for mutation in mutations)
    _require_source_contract(
        actual_mutations == expected_mutations,
        "catalog lifecycle DML inventory drifted",
    )
    binding_counts = Counter(binding.site_id for binding in bindings)
    _require_source_contract(
        binding_counts == Counter(sites_by_id.keys()),
        "catalog lifecycle helper binding inventory drifted",
    )
    mutations_by_identity = {mutation.identity: mutation for mutation in mutations}
    for binding in bindings:
        site = sites_by_id[binding.site_id]
        _require_source_contract(
            (binding.module, binding.function) == (site.module, site.function),
            f"catalog lifecycle helper {site.site_id!r} moved outside its writer",
        )
        _require_source_contract(
            binding.keyword_names
            >= frozenset({"previous_state", "next_state", "timestamp"}),
            f"catalog lifecycle helper {site.site_id!r} omits an edge argument",
        )
        if binding.previous_state is not _UNRESOLVED:
            _require_source_contract(
                any(
                    previous == binding.previous_state
                    for previous, _next in site.transitions
                ),
                f"catalog lifecycle helper {site.site_id!r} changed previous state",
            )
        if binding.next_state is not _UNRESOLVED:
            _require_source_contract(
                any(
                    next_state == binding.next_state
                    for _previous, next_state in site.transitions
                ),
                f"catalog lifecycle helper {site.site_id!r} changed next state",
            )
        if (
            binding.previous_state is not _UNRESOLVED
            and binding.next_state is not _UNRESOLVED
        ):
            _require_source_contract(
                (binding.previous_state, binding.next_state) in site.transitions,
                f"catalog lifecycle helper {site.site_id!r} changed its edge",
            )
        machine = machines[site.machine]
        timestamp_presence = frozenset(
            None
            if machine.timestamp_attribute is None
            else next_state in machine.timestamp_states
            for _previous, next_state in site.transitions
        )
        if timestamp_presence in {frozenset({None}), frozenset({False})}:
            _require_source_contract(
                binding.timestamp is None,
                f"catalog lifecycle helper {site.site_id!r} changed absent timestamp",
            )
        elif timestamp_presence == frozenset({True}):
            _require_source_contract(
                binding.timestamp is not None,
                f"catalog lifecycle helper {site.site_id!r} omitted required timestamp",
            )

        mutation = mutations_by_identity[
            (site.module, site.function, site.table, site.operation)
        ]
        _require_source_contract(
            binding.lineno < mutation.lineno,
            f"catalog lifecycle helper {site.site_id!r} runs after its mutation",
        )
        if site.state_column is not None:
            _require_source_contract(
                _assignment_mentions_column(
                    mutation.query,
                    site.operation,
                    site.state_column,
                ),
                f"catalog lifecycle state writer {site.site_id!r} omitted its SET",
            )
            next_index = _assignment_parameter_index(
                mutation.query,
                site.operation,
                site.state_column,
            )
            _require_source_contract(
                next_index is not None,
                f"catalog lifecycle state writer {site.site_id!r} has unsupported SET",
            )
            _require_source_contract(
                binding.target is not None,
                f"catalog lifecycle state writer {site.site_id!r} discarded validation",
            )
            assert next_index is not None
            assert binding.target is not None
            _require_source_contract(
                _parameter_is_attribute(
                    mutation.parameters,
                    next_index,
                    binding.target,
                    "next_state",
                ),
                f"catalog lifecycle state writer {site.site_id!r} bypassed next state",
            )
            if site.operation == "UPDATE":
                assert mutation.query is not None
                previous_index = _update_parameter_index(
                    mutation.query,
                    site.state_column,
                    where=True,
                )
                _require_source_contract(
                    previous_index is not None,
                    f"catalog lifecycle state writer {site.site_id!r} has unsupported WHERE",
                )
                assert previous_index is not None
                _require_source_contract(
                    _parameter_is_attribute(
                        mutation.parameters,
                        previous_index,
                        binding.target,
                        "previous_state",
                    ),
                    f"catalog lifecycle state writer {site.site_id!r} bypassed previous state",
                )
        if site.timestamp_column is not None:
            _require_source_contract(
                _assignment_mentions_column(
                    mutation.query,
                    site.operation,
                    site.timestamp_column,
                ),
                f"catalog lifecycle writer {site.site_id!r} omitted its timestamp SET",
            )
            timestamp_index = _assignment_parameter_index(
                mutation.query,
                site.operation,
                site.timestamp_column,
            )
            _require_source_contract(
                timestamp_index is not None,
                f"catalog lifecycle writer {site.site_id!r} has unsupported timestamp SET",
            )
            _require_source_contract(
                binding.target is not None,
                f"catalog lifecycle writer {site.site_id!r} discarded timestamp validation",
            )
            assert timestamp_index is not None
            assert binding.target is not None
            _require_source_contract(
                _parameter_is_attribute(
                    mutation.parameters,
                    timestamp_index,
                    binding.target,
                    "timestamp",
                ),
                f"catalog lifecycle writer {site.site_id!r} bypassed timestamp",
            )
        if site.timestamp_sink is not None:
            sink = site.timestamp_sink
            forward_calls = tuple(
                call
                for call in calls
                if (call.module, call.function, call.name)
                == (site.module, site.function, sink.function)
            )
            _require_source_contract(
                len(forward_calls) == 1,
                f"catalog lifecycle marker {site.site_id!r} timestamp forwarding drifted",
            )
            forward = forward_calls[0]
            _require_source_contract(
                binding.target is not None
                and _is_required_timestamp_forward(
                    _call_keyword(forward, sink.keyword),
                    binding.target,
                ),
                f"catalog lifecycle marker {site.site_id!r} bypassed derived timestamp",
            )
            _require_source_contract(
                binding.lineno < forward.lineno < mutation.lineno,
                f"catalog lifecycle marker {site.site_id!r} timestamp ordering drifted",
            )
            sink_mutation = mutations_by_identity[
                (site.module, sink.function, sink.table, sink.operation)
            ]
            _require_source_contract(
                _assignment_mentions_column(
                    sink_mutation.query,
                    sink.operation,
                    sink.column,
                ),
                f"catalog lifecycle marker {site.site_id!r} omitted timestamp sink",
            )
            sink_index = _assignment_parameter_index(
                sink_mutation.query,
                sink.operation,
                sink.column,
            )
            _require_source_contract(
                sink_index is not None,
                f"catalog lifecycle marker {site.site_id!r} timestamp sink is unsupported",
            )
            assert sink_index is not None
            _require_source_contract(
                _parameter_is_name(
                    sink_mutation.parameters,
                    sink_index,
                    sink.keyword,
                ),
                f"catalog lifecycle marker {site.site_id!r} misplaced timestamp sink",
            )


def test_catalog_state_machine_contract_matches_generated_schema_and_manifest() -> None:
    validate_catalog_state_machine_contract()
    with (ROOT / "verification" / "schema" / "catalog.toml").open("rb") as stream:
        manifest = tomllib.load(stream)
    authority = cast(dict[str, object], manifest["transition_authority_contract"])
    assert tuple(cast(list[str], authority["gate_relations"])) == (
        CATALOG_STATE_MACHINE_TRANSITION_GATES
    )
    obligation = next(
        value
        for value in cast(list[dict[str, object]], manifest["semantic_obligation"])
        if value["id"] == "catalog.state-machines.v1"
    )
    assert tuple(cast(list[str], obligation["relations"])) == (
        CATALOG_STATE_MACHINE_GATE_RELATIONS
    )


def test_catalog_transition_and_timestamp_matrix_is_closed() -> None:
    for machine in CATALOG_STATE_MACHINES:
        previous_values: tuple[str | None, ...] = (
            None,
            *sorted(machine.states),
            "UNREGISTERED",
        )
        next_values = (*sorted(machine.states), "UNREGISTERED")
        for previous_state in previous_values:
            for next_state in next_values:
                for timestamp_present in (None, False, True):
                    expected_timestamp = (
                        timestamp_present is None
                        if machine.timestamp_attribute is None
                        else timestamp_present
                        is (next_state in machine.timestamp_states)
                    )
                    expected = (
                        previous_state,
                        next_state,
                    ) in machine.transitions and expected_timestamp
                    actual = catalog_transition_is_valid(
                        machine.name,
                        previous_state=previous_state,
                        next_state=next_state,
                        timestamp_present=timestamp_present,
                    )
                    assert actual is expected
                    if expected:
                        require_catalog_transition(
                            machine.name,
                            previous_state=previous_state,
                            next_state=next_state,
                            timestamp_present=timestamp_present,
                        )
                    else:
                        with pytest.raises(CatalogStateMachineContractError):
                            require_catalog_transition(
                                machine.name,
                                previous_state=previous_state,
                                next_state=next_state,
                                timestamp_present=timestamp_present,
                            )


def test_catalog_state_mutation_site_registry_is_closed_world() -> None:
    _validate_catalog_state_mutation_sources()


def test_site_bound_transition_matrix_rejects_other_legal_edges_and_timestamps() -> (
    None
):
    machines = {machine.name: machine for machine in CATALOG_STATE_MACHINES}
    for site in CATALOG_STATE_MUTATION_SITES:
        machine = machines[site.machine]
        for previous_state, next_state in machine.transitions:
            timestamp = (
                1
                if machine.timestamp_attribute is not None
                and next_state in machine.timestamp_states
                else None
            )
            if (previous_state, next_state) in site.transitions:
                transition = require_catalog_state_mutation(
                    site.site_id,
                    previous_state=previous_state,
                    next_state=next_state,
                    timestamp=timestamp,
                )
                assert transition.site is site
                assert transition.previous_state == previous_state
                assert transition.next_state == next_state
                assert transition.timestamp == timestamp
                with pytest.raises(CatalogStateMachineContractError):
                    require_catalog_state_mutation(
                        site.site_id,
                        previous_state=previous_state,
                        next_state=next_state,
                        timestamp=1 if timestamp is None else None,
                    )
            else:
                with pytest.raises(CatalogStateMachineContractError):
                    require_catalog_state_mutation(
                        site.site_id,
                        previous_state=previous_state,
                        next_state=next_state,
                        timestamp=timestamp,
                    )
    with pytest.raises(CatalogStateMachineContractError, match="unknown"):
        require_catalog_state_mutation(
            "unregistered-site",
            previous_state=None,
            next_state="OPEN",
            timestamp=None,
        )


@pytest.mark.parametrize(
    ("module", "original", "replacement", "message"),
    (
        (
            "h2hdb.vnext_artifact_release_repository",
            "transition.next_state,",
            '"COMMITTED",',
            "bypassed next state",
        ),
        (
            "h2hdb.vnext_artifact_release_repository",
            "transition.previous_state,",
            '"PREPARED",',
            "bypassed previous state",
        ),
        (
            "h2hdb.vnext_artifact_release_repository",
            "transition.next_state,\n                    item.candidate_id,",
            "item.candidate_id,\n                    transition.next_state,",
            "bypassed next state",
        ),
        (
            "h2hdb.vnext_artifact_release_repository",
            "UPDATE catalog_prepared_artifacts SET state = %s ",
            "UPDATE catalog_prepared_artifacts SET storage_generation = %s ",
            "omitted its SET",
        ),
        (
            "h2hdb.vnext_artifact_release_repository",
            "AND resource_kind = %s AND state = %s",
            "AND resource_kind = %s AND storage_generation = %s",
            "unsupported WHERE",
        ),
        (
            "h2hdb.vnext_manifest_family",
            "(proposed.build_id, transition.timestamp),",
            "(proposed.build_id, proposed.computed_at),",
            "bypassed timestamp",
        ),
        (
            "h2hdb.vnext_manifest_family",
            "(proposed.build_id, transition.timestamp),",
            "(transition.timestamp, proposed.build_id),",
            "bypassed timestamp",
        ),
        (
            "h2hdb.vnext_manifest_family",
            "(build_id, sealed_at) VALUES (%s, %s)",
            "(build_id, computed_at) VALUES (%s, %s)",
            "omitted its timestamp SET",
        ),
        (
            "h2hdb.vnext_publication_finalization_repository",
            "publication_transition.required_timestamp",
            "timestamp",
            "bypassed derived timestamp",
        ),
        (
            "h2hdb.vnext_publication_finalization_repository",
            "len(page.items),\n            committed_at,",
            "len(page.items),\n            page.publication_committed_at,",
            "misplaced timestamp sink",
        ),
        (
            "h2hdb.vnext_artifact_preparation_repository",
            '"catalog_publication_candidate_projection_seals",',
            '"catalog_publication_candidate_projection_seals_drift",',
            "DML inventory drifted",
        ),
    ),
)
def test_source_contract_mutations_break_state_timestamp_and_marker_coupling(
    module: str,
    original: str,
    replacement: str,
    message: str,
) -> None:
    path = ROOT / "src" / Path(*module.split("."))
    path = path.with_suffix(".py")
    source = path.read_text(encoding="utf-8")
    assert source.count(original) == 1
    mutated = source.replace(original, replacement, 1)
    with pytest.raises(AssertionError, match=message):
        _validate_catalog_state_mutation_sources({module: mutated})


def test_source_contract_tracks_execute_aliases_without_flagging_benign_sql() -> None:
    module = "h2hdb.vnext_artifact_release_repository"
    path = ROOT / "src" / Path(*module.split("."))
    source = path.with_suffix(".py").read_text(encoding="utf-8")
    benign = (
        source
        + """

def _benign_execute_aliases(connector, execute_alias):
    select_alias = connector.execute
    select_alias("SELECT state FROM catalog_prepared_artifacts", ())
    update_alias = connector.execute_affected
    update_alias("UPDATE unrelated_table SET state = %s", ("READY",))
    execute_alias("SELECT 1")
"""
    )
    _validate_catalog_state_mutation_sources({module: benign})

    mutation = (
        benign
        + """

def _unregistered_state_write(connector, candidate_id):
    execute_alias = connector.execute
    execute_alias(
        "UPDATE catalog_prepared_artifacts SET state = %s "
        "WHERE candidate_id = %s AND state = %s",
        ("COMMITTED", candidate_id, "PENDING"),
    )
"""
    )
    with pytest.raises(AssertionError, match="DML inventory drifted"):
        _validate_catalog_state_mutation_sources({module: mutation})


@pytest.mark.parametrize(
    ("alias", "method"),
    (("execute_alias", "compare_and_swap"), ("execute", "execute")),
)
def test_source_contract_rejects_execute_alias_with_dynamic_sql(
    alias: str,
    method: str,
) -> None:
    module = "h2hdb.vnext_artifact_release_repository"
    path = ROOT / "src" / Path(*module.split("."))
    source = path.with_suffix(".py").read_text(encoding="utf-8")
    mutation = (
        source
        + f"""

def _unregistered_dynamic_state_write(connector, statement, parameters):
    {alias} = connector.{method}
    {alias}(statement, parameters)
"""
    )
    with pytest.raises(AssertionError, match="cannot be resolved statically"):
        _validate_catalog_state_mutation_sources({module: mutation})


def test_source_contract_rejects_authority_dml_on_unknown_call_shape() -> None:
    module = "h2hdb.vnext_artifact_release_repository"
    path = ROOT / "src" / Path(*module.split("."))
    source = path.with_suffix(".py").read_text(encoding="utf-8")
    mutation = (
        source
        + """

def _unregistered_wrapped_state_write(run_sql, candidate_id):
    run_sql(
        "UPDATE catalog_prepared_artifacts SET state = %s "
        "WHERE candidate_id = %s AND state = %s",
        ("COMMITTED", candidate_id, "PENDING"),
    )
"""
    )
    with pytest.raises(AssertionError, match="unsupported call shape"):
        _validate_catalog_state_mutation_sources({module: mutation})


def test_catalog_state_writer_binding_matches_central_registry() -> None:
    binding = catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS["catalog.state-machines.v1"]
    installed = frozenset(
        f"{entrypoint.__module__}.{entrypoint.__qualname__}"
        for entrypoint in binding.entrypoints
    )
    assert installed == CATALOG_STATE_MACHINE_WRITER_ENTRYPOINTS


def test_generated_state_enum_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backends = dict(cast(Mapping[str, Any], ARTIFACT["backends"]))
    sqlite = dict(cast(Mapping[str, Any], backends["sqlite"]))
    relations = list(cast(tuple[Mapping[str, Any], ...], sqlite["relations"]))
    index = next(
        offset
        for offset, relation in enumerate(relations)
        if relation["relation"] == "analysis_run_state"
    )
    changed = dict(relations[index])
    changed["checks"] = (("ck_drift", "length(analysis_id) = 16"),)
    relations[index] = changed
    sqlite["relations"] = tuple(relations)
    backends["sqlite"] = sqlite
    monkeypatch.setitem(ARTIFACT, "backends", backends)

    with pytest.raises(
        CatalogStateMachineContractError,
        match="state enum check",
    ):
        validate_catalog_state_machine_contract()


def test_concrete_lifecycle_families_reject_timestamp_drift() -> None:
    with pytest.raises(ValueError, match="sealed_at"):
        SourceBuildFamily(bytes(16), bytes(32), 1, "SEALED", 2, None)
    with pytest.raises(ValueError, match="has sealed_at"):
        SourceBuildFamily(bytes(16), bytes(32), 1, "ABANDONED", 2, 3)
    with pytest.raises(ValueError, match="completed_at"):
        AnalysisRunFamily(bytes(16), bytes(16), 1, bytes(32), 2, "COMPLETE", None)
    with pytest.raises(ValueError, match="has completed_at"):
        AnalysisRunFamily(bytes(16), bytes(16), 1, bytes(32), 2, "ABANDONED", 3)


def test_shared_cas_helpers_reject_unregistered_edges_before_database_access() -> None:
    unusable_work: Any = object()
    with pytest.raises(CatalogStateMachineContractError, match="rejected transition"):
        cas_analysis_run_state(
            unusable_work,
            analysis_id=bytes(16),
            previous="COMPLETE",
            successor="OPEN",
            timestamp=None,
            authority="negative edge",
        )
    with pytest.raises(CatalogStateMachineContractError, match="rejected transition"):
        cas_prepared_artifact_state(
            unusable_work,
            candidate_id=bytes(16),
            publication_key=bytes(32),
            resource_kind=CatalogResourceKind.ACQUISITION,
            expected_state="COMMITTED",
            next_state="PENDING",
        )
