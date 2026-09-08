from __future__ import annotations

import pytest

from h2hdb import ArtifactFailureContext, get_artifact_failure_context
from h2hdb.artifact_errors import artifact_failure_scope


def test_context_survives_cause_wrapping_with_safe_bounded_notes() -> None:
    context = ArtifactFailureContext(7, ("資料",), ("作品\nname",))
    original = RuntimeError("storage unavailable")
    with pytest.raises(RuntimeError) as caught, artifact_failure_scope(context):
        raise original
    assert caught.value is original
    assert all("\n" not in note for note in original.__notes__)
    wrapper = ValueError("outer adapter failure")
    wrapper.__cause__ = original
    assert get_artifact_failure_context(wrapper) == context


def test_context_lookup_terminates_on_cyclic_cause_chain() -> None:
    error = RuntimeError("cycle")
    error.__cause__ = error
    assert get_artifact_failure_context(error) is None


def test_inner_member_context_is_not_replaced_by_outer_gallery_context() -> None:
    gallery = ArtifactFailureContext(7, ("root",), ("gallery",))
    member = ArtifactFailureContext(7, ("root",), ("gallery",), b"page.jpg", 12)
    with (
        pytest.raises(RuntimeError) as caught,
        artifact_failure_scope(gallery),
        artifact_failure_scope(member),
    ):
        raise RuntimeError("cannot read source")
    assert get_artifact_failure_context(caught.value) == member


def test_cancellation_retains_original_class_without_attached_context() -> None:
    context = ArtifactFailureContext(7, ("root",), ("gallery",))
    error = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt) as caught, artifact_failure_scope(context):
        raise error
    assert caught.value is error
    assert get_artifact_failure_context(error) is None
