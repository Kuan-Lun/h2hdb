from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from hypothesis import given, settings
from hypothesis import strategies as st

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "verification" / "schema" / "check_contract.py"
ATTRIBUTES = ("a", "b", "c", "d", "e")


type FDSystem = tuple[frozenset[str], tuple[Any, ...], frozenset[str], frozenset[str]]


def _load_checker() -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "h2hdb_schema_property_checker", CHECKER
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


checker = cast(Any, _load_checker())


@st.composite
def _fd_system(draw: st.DrawFn) -> FDSystem:
    attributes = frozenset(
        draw(st.sets(st.sampled_from(ATTRIBUTES), min_size=1, max_size=5))
    )
    subsets = st.frozensets(st.sampled_from(tuple(sorted(attributes))))
    pairs = draw(st.lists(st.tuples(subsets, subsets), max_size=12))
    dependencies = tuple(
        checker.FunctionalDependency(determinant, dependent)
        for determinant, dependent in pairs
    )
    seed = frozenset(draw(st.sets(st.sampled_from(tuple(sorted(attributes))))))
    second_seed = frozenset(draw(st.sets(st.sampled_from(tuple(sorted(attributes))))))
    return attributes, dependencies, seed, second_seed


@settings(max_examples=250, deadline=None, derandomize=True)
@given(system=_fd_system())
def test_generated_attribute_closure_obeys_closure_axioms(system: FDSystem) -> None:
    attributes, dependencies, first, second = system
    first_closure = checker.attribute_closure(first, dependencies)

    # Extensivity and domain closure.
    assert first <= first_closure <= attributes
    # Idempotence.
    assert checker.attribute_closure(first_closure, dependencies) == first_closure
    # Monotonicity.
    union = first | second
    assert first_closure <= checker.attribute_closure(union, dependencies)

    # Every supplied implication whose determinant is reached is satisfied.
    for dependency in dependencies:
        if dependency.determinant <= first_closure:
            assert dependency.dependent <= first_closure


@settings(max_examples=150, deadline=None, derandomize=True)
@given(system=_fd_system())
def test_generated_candidate_keys_are_exactly_minimal_superkeys(
    system: FDSystem,
) -> None:
    attributes, dependencies, _first, _second = system
    keys = checker.enumerate_candidate_keys(attributes, dependencies)

    assert keys
    assert len(set(keys)) == len(keys)
    for key in keys:
        assert checker.attribute_closure(key, dependencies) == attributes
        assert all(
            checker.attribute_closure(key - {attribute}, dependencies) != attributes
            for attribute in key
        )

    # Exhaustively ensure the enumerator did not omit a minimal superkey.
    for candidate_mask in range(1 << len(attributes)):
        ordered = tuple(sorted(attributes))
        candidate = frozenset(
            attribute
            for index, attribute in enumerate(ordered)
            if candidate_mask & (1 << index)
        )
        if checker.attribute_closure(candidate, dependencies) != attributes:
            continue
        if any(
            checker.attribute_closure(candidate - {attribute}, dependencies)
            == attributes
            for attribute in candidate
        ):
            continue
        assert candidate in keys
