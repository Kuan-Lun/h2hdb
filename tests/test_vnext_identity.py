from __future__ import annotations

import hashlib
import inspect
import random
from dataclasses import fields

import pytest

from h2hdb.vnext_identity import (
    ANALYSIS_STATE_COMPONENTS,
    ARTIFACT_COMPONENT_CODEC_VERSION,
    ARTIFACT_COMPONENT_KINDS,
    ARTIFACT_MEMBER_PLAN_VERSION,
    ARTIFACT_POLICY_CODEC_VERSION,
    ARTIFACT_PRODUCER_FINGERPRINT_CODEC_VERSION,
    ARTIFACT_PROTECTION_TOKEN_CODEC_VERSION,
    ARTIFACT_STORAGE_KEY_CODEC_VERSION,
    ARTIFACT_STORAGE_KEY_MAXIMUM_BYTES,
    CANONICAL_VALUE_CHUNK_BYTES,
    EFFECTIVE_CONTENT_ENCODING_VERSION,
    FILESYSTEM_STAT_FINGERPRINT_BYTES,
    GALLERY_OBSERVATION_DIRECTORY_LEAF_CAPACITY,
    GALLERY_OBSERVATION_DURABLE_PARSER_PHASES,
    GALLERY_OBSERVATION_FILE_LEAF_CAPACITY,
    GALLERY_OBSERVATION_METADATA_CHUNK_BYTES,
    GALLERY_OBSERVATION_TAG_LEAF_CAPACITY,
    SOURCE_LOCATOR_CODEC_VERSION,
    SOURCE_PROVIDERS,
    SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION,
    AnalysisTitleScalarReceipt,
    ArtifactMemberEntryKind,
    ArtifactMemberPlanEntry,
    ArtifactProtectionToken,
    ArtifactSourceRole,
    ArtifactTransformKind,
    ByteDomainError,
    CanonicalIdentityCollisionError,
    CanonicalValueChunk,
    CanonicalValuePage,
    CanonicalValueTree,
    DigestFormatError,
    DigestMismatchError,
    GalleryObservationComponent,
    GalleryObservationDescriptor,
    GalleryObservationDirectoryEntry,
    GalleryObservationDirectoryFileType,
    GalleryObservationEncodedPage,
    GalleryObservationFileEntry,
    GalleryObservationMetadata,
    GalleryObservationMetadataChunk,
    GalleryObservationMetadataDecoder,
    GalleryObservationMetadataDecoderState,
    GalleryObservationNodeKind,
    GalleryObservationPage,
    GalleryObservationTagEntry,
    GalleryObservationTree,
    IntegerDomainError,
    RegisteredIdentifierError,
    SourceRelativeLocatorValidationReceipt,
    SourceRootValidationReceipt,
    SourceSnapshotContentOwner,
    SourceSnapshotCounts,
    SourceSnapshotFileHashDecision,
    SourceSnapshotGallery,
    SourceSnapshotGidWinner,
    SourceSnapshotPolicy,
    StrictUtf8ScalarCounter,
    analysis_candidate_has_already_uploaded,
    artifact_archive_member_name,
    artifact_effective_content_digest,
    artifact_effective_content_digest_ordered,
    artifact_id,
    artifact_member_plan_digest,
    artifact_member_plan_digest_ordered,
    artifact_name,
    artifact_owner_digest,
    artifact_policy_digest,
    artifact_producer_fingerprint_sha256,
    artifact_selected_digest,
    artifact_semantics_digest,
    artifact_source_manifest_digest,
    artifact_storage_key_components,
    artifact_storage_key_digest,
    artifact_storage_receipt_id,
    build_canonical_value_tree,
    build_gallery_observation_metadata_tree,
    build_gallery_observation_tree,
    canonical_value_digest,
    canonical_value_digest_hex,
    canonical_value_digest_parts,
    canonical_value_page_digest,
    catalog_language_digest,
    catalog_language_digest_parts,
    catalog_summary_digest,
    catalog_summary_digest_parts,
    count_analysis_title_scalars,
    decode_artifact_id,
    decode_artifact_member_plan,
    decode_artifact_name,
    decode_artifact_protection_token,
    decode_artifact_storage_key,
    decode_canonical_value_page,
    decode_filesystem_stat_fingerprint,
    decode_gallery_observation_descriptor,
    decode_gallery_observation_metadata,
    decode_gallery_observation_page,
    decode_publication_id,
    decode_source_relative_locator,
    decode_source_root,
    digest_from_hex,
    digest_to_hex,
    effective_content_digest,
    effective_content_digest_ordered,
    encode_artifact_effective_content,
    encode_artifact_member_plan,
    encode_artifact_owner,
    encode_artifact_policy,
    encode_artifact_producer_fingerprint,
    encode_artifact_protection_token,
    encode_artifact_selected,
    encode_artifact_semantics,
    encode_artifact_source_manifest,
    encode_artifact_storage_key,
    encode_effective_content,
    encode_filesystem_stat_fingerprint,
    encode_gallery_observation_descriptor,
    encode_gallery_observation_metadata,
    encode_gallery_observation_page,
    encode_source_relative_locator,
    encode_source_root,
    encode_source_snapshot_manifest,
    encode_zip_comment,
    file_key,
    file_role,
    gallery_directory_audit_digest,
    gallery_key,
    gallery_key_hex,
    gallery_metadata_audit_digest,
    gallery_observation_descriptor_digest,
    gallery_observation_page_digest,
    gallery_observation_page_key_bounds,
    gallery_scan_audit_digest,
    iter_artifact_effective_content_payload_ordered,
    iter_artifact_member_plan_payload,
    iter_artifact_storage_key_payload,
    iter_decode_artifact_storage_key,
    iter_effective_content_payload_ordered,
    iter_gallery_observation_metadata_stream,
    iter_source_relative_locator_payload,
    iter_source_root_payload,
    iter_source_snapshot_manifest_payload_ordered,
    iter_source_snapshot_manifest_payload_rows_ordered,
    publication_id,
    publication_key,
    publication_key_hex,
    source_relative_locator_digest,
    source_root_digest,
    source_scope_key,
    source_snapshot_manifest_digest,
    source_snapshot_manifest_digest_ordered,
    validate_artifact_component_kind,
    validate_canonical_value_identity,
    validate_canonical_value_tree,
    validate_file_name,
    validate_gallery_name,
    validate_gallery_observation_durable_parser_phase,
    validate_gallery_observation_metadata_parts,
    validate_gallery_observation_tree,
    validate_namespace,
    validate_registered_ascii_identifier,
    validate_source_relative_locator_parts,
    validate_source_root_parts,
    validate_state_component,
    verify_canonical_value_conflict,
    verify_canonical_value_page_conflict,
    verify_gallery_observation_page_conflict,
)


def _manual_canonical_digest(digest_domain: str, payload: bytes) -> bytes:
    domain = digest_domain.encode("ascii")
    preimage = (
        b"h2hdb-vnext-canonical-value\0"
        + (1).to_bytes(4, "big")
        + len(domain).to_bytes(4, "big")
        + domain
        + len(payload).to_bytes(8, "big")
        + payload
    )
    return hashlib.sha256(preimage).digest()


def _manual_gallery_key(version: int, scope_key: bytes, locator_sha256: bytes) -> bytes:
    preimage = (
        b"h2hdb-vnext-gallery-key\0"
        + version.to_bytes(4, "big")
        + scope_key
        + locator_sha256
    )
    return hashlib.sha256(preimage).digest()


def test_filesystem_stat_fingerprint_is_exact_fixed_width_and_round_trips() -> None:
    facts = (
        (1 << 64) - 1,
        7,
        (1 << 63) - 1,
        -(1 << 63),
        (1 << 63) - 1,
    )
    encoded = encode_filesystem_stat_fingerprint(
        device=facts[0],
        inode=facts[1],
        size_bytes=facts[2],
        modified_ns=facts[3],
        changed_ns=facts[4],
    )
    assert len(encoded) == FILESYSTEM_STAT_FINGERPRINT_BYTES == 40
    assert encoded.hex() == (
        "ffffffffffffffff00000000000000077fffffffffffffff"
        "80000000000000007fffffffffffffff"
    )
    assert decode_filesystem_stat_fingerprint(encoded) == facts

    for malformed in (b"", encoded[:-1], encoded + b"x"):
        with pytest.raises(ByteDomainError, match="exactly 40 bytes"):
            decode_filesystem_stat_fingerprint(malformed)
    with pytest.raises(IntegerDomainError, match="size_bytes"):
        encode_filesystem_stat_fingerprint(
            device=0,
            inode=0,
            size_bytes=1 << 63,
            modified_ns=0,
            changed_ns=0,
        )


def test_artifact_storage_key_and_publication_payload_domains_are_exact() -> None:
    components = ("sha256", "ab.cbz")
    payload = encode_artifact_storage_key(components)
    assert ARTIFACT_STORAGE_KEY_CODEC_VERSION == 1
    assert payload.hex() == ("0000000100000002000000067368613235360000000661622e63627a")
    assert b"".join(iter_artifact_storage_key_payload(components)) == payload
    assert decode_artifact_storage_key(payload) == components
    for split in range(len(payload) + 1):
        assert (
            tuple(iter_decode_artifact_storage_key((payload[:split], payload[split:])))
            == components
        )
    assert artifact_storage_key_digest(components) == _manual_canonical_digest(
        "artifact_storage_key_bytes_v1", payload
    )

    summary = "摘要é".encode()
    assert catalog_summary_digest(summary) == _manual_canonical_digest(
        "catalog_summary_utf8_v1", summary
    )
    assert catalog_summary_digest(b"") == _manual_canonical_digest(
        "catalog_summary_utf8_v1", b""
    )
    assert catalog_language_digest(b"und") == _manual_canonical_digest(
        "catalog_language_utf8_v1", b"und"
    )
    assert catalog_summary_digest_parts(
        len(summary), (summary[:4], summary[4:5], summary[5:])
    ) == catalog_summary_digest(summary)
    assert catalog_language_digest_parts(3, (b"u", b"n", b"d")) == (
        catalog_language_digest(b"und")
    )
    assert catalog_summary_digest("é".encode()) != catalog_summary_digest(
        "e\N{COMBINING ACUTE ACCENT}".encode()
    )

    for invalid in (b"\xff", b"\xc3"):
        with pytest.raises(ByteDomainError, match="exact UTF-8"):
            catalog_summary_digest(invalid)
    with pytest.raises(ByteDomainError, match="must not be empty"):
        catalog_language_digest(b"")
    with pytest.raises(ByteDomainError, match="exact UTF-8"):
        catalog_summary_digest_parts(2, (b"\xc3", b"x"))
    with pytest.raises(ByteDomainError, match="declared_byte_count"):
        catalog_summary_digest_parts(1, (b"ab",))
    with pytest.raises(ByteDomainError, match="separator|direct-child"):
        encode_artifact_storage_key(("bad/segment",))
    with pytest.raises(ByteDomainError, match="truncated"):
        tuple(iter_decode_artifact_storage_key((payload[:-1],)))
    with pytest.raises(ByteDomainError, match="trailing"):
        tuple(iter_decode_artifact_storage_key((payload, b"x")))
    bad = iter_decode_artifact_storage_key((payload, b"x"))
    with pytest.raises(ByteDomainError, match="trailing"):
        next(bad)
    maximum = (*("x" * 255 for _ in range(15)), "x" * 199)
    assert (
        len(encode_artifact_storage_key(maximum)) == ARTIFACT_STORAGE_KEY_MAXIMUM_BYTES
    )
    with pytest.raises(ByteDomainError, match="exceeds 4096 bytes"):
        encode_artifact_storage_key((*maximum, "x"))
    with pytest.raises(ByteDomainError, match="exceeds 4096 bytes"):
        next(iter_decode_artifact_storage_key((bytes(4097),)))


def test_artifact_storage_key_is_derived_only_from_positive_gid() -> None:
    components = artifact_storage_key_components(7)
    payload = encode_artifact_storage_key(components)

    assert components == ("hash-v1", "bd", "2", "h2h-7.cbz")
    assert payload.hex() == (
        "000000010000000400000007686173682d76310000000262640000000132"
        "000000096832682d372e63627a"
    )
    assert artifact_storage_key_digest(components).hex() == (
        "1b3415259ae661635a171998d34955b9ddc083c740287a5025387e3d9136a2b7"
    )

    for invalid in (0, -1, 1 << 63, True, bytes(32)):
        with pytest.raises(IntegerDomainError):
            artifact_storage_key_components(invalid)  # type: ignore[arg-type]


def _manual_locator(components: tuple[str, ...]) -> bytes:
    return b"".join(
        (
            SOURCE_LOCATOR_CODEC_VERSION.to_bytes(4, "big"),
            len(components).to_bytes(4, "big"),
            *(
                len(component.encode()).to_bytes(4, "big") + component.encode()
                for component in components
            ),
        )
    )


def test_canonical_value_digest_matches_independent_golden_framing() -> None:
    payload = "原樣 A ".encode()
    expected = bytes.fromhex(
        "e4157463b3ab4d843f097f29592f3ab7fed90ed815009de2ca33f90b8ebe3814"
    )

    actual = canonical_value_digest("test_golden_v1", payload)

    assert actual == expected
    assert actual == _manual_canonical_digest("test_golden_v1", payload)
    assert type(actual) is bytes
    assert len(actual) == 32
    assert canonical_value_digest_hex("test_golden_v1", payload) == expected.hex()


def test_canonical_digest_is_domain_separated_and_exact() -> None:
    assert canonical_value_digest("domain_a_v1", b"A") != canonical_value_digest(
        "domain_b_v1", b"A"
    )
    assert canonical_value_digest("domain_a_v1", b"A") != canonical_value_digest(
        "domain_a_v1", b"a"
    )
    assert canonical_value_digest("domain_a_v1", b"A") != canonical_value_digest(
        "domain_a_v1", b"A "
    )
    assert canonical_value_digest("domain_a_v1", b"") == _manual_canonical_digest(
        "domain_a_v1", b""
    )
    assert canonical_value_digest("domain_a_v1", b"\x00") == _manual_canonical_digest(
        "domain_a_v1", b"\x00"
    )


def test_effective_content_matches_hardcoded_duplicate_preserving_golden() -> None:
    low = bytes.fromhex("00" * 32)
    high = bytes.fromhex("ff" * 32)
    expected_payload = bytes.fromhex(
        "68326864622d766e6578742d6566666563746976652d636f6e74656e7400"
        "00000001"
        "0000000000000003" + "00" * 32 + "ff" * 32 + "ff" * 32
    )
    expected_digest = bytes.fromhex(
        "0982d9878d0e9434b6654265245951277defe525e8c0f925808d16373db204c8"
    )

    actual_payload = encode_effective_content([high, low, high])

    assert EFFECTIVE_CONTENT_ENCODING_VERSION == 1
    assert actual_payload == expected_payload
    assert b"".join(iter_effective_content_payload_ordered(3, [low, high, high])) == (
        expected_payload
    )
    assert effective_content_digest_ordered(3, iter([low, high, high])) == (
        effective_content_digest([high, low, high])
    )
    assert effective_content_digest([high, low, high]) == expected_digest
    assert encode_effective_content([high, high, low]) == expected_payload
    assert effective_content_digest([low, high, high]) == expected_digest
    assert encode_effective_content([low, high]) != expected_payload
    assert effective_content_digest([low, high]) != expected_digest


def test_effective_content_rejects_non_sha32_and_unregistered_versions() -> None:
    with pytest.raises(DigestFormatError):
        encode_effective_content([bytes(31)])
    with pytest.raises(DigestFormatError):
        encode_effective_content([bytearray(32)])  # type: ignore[list-item]
    with pytest.raises(IntegerDomainError):
        encode_effective_content([], encoding_version=0)
    with pytest.raises(IntegerDomainError):
        effective_content_digest([], encoding_version=2)


def test_effective_content_empty_sequence_has_one_exact_identity() -> None:
    expected = (
        b"h2hdb-vnext-effective-content\0"
        + (1).to_bytes(4, "big")
        + (0).to_bytes(8, "big")
    )

    assert encode_effective_content([]) == expected
    assert effective_content_digest([]) == _manual_canonical_digest(
        "effective_content_v1", expected
    )


def test_ordered_effective_content_streams_fail_closed_on_order_or_count_drift() -> (
    None
):
    low = bytes(32)
    high = bytes([255]) * 32
    with pytest.raises(ByteDomainError, match="not ordered"):
        effective_content_digest_ordered(2, iter((high, low)))
    with pytest.raises(ByteDomainError, match="declared file_count"):
        artifact_effective_content_digest_ordered(2, iter((low,)))


def test_analysis_title_scalar_counter_streams_strict_utf8_with_fixed_receipt() -> None:
    title = "A😀é".encode()
    receipt = count_analysis_title_scalars(
        (title[:2], title[2:4], title[4:6], title[6:])
    )
    assert receipt == AnalysisTitleScalarReceipt(byte_count=7, scalar_count=3)

    counter = StrictUtf8ScalarCounter()
    counter.feed(b"A\xf0\x9f")
    with pytest.raises(ByteDomainError, match="ends inside a UTF-8 scalar"):
        counter.finalize()


def test_analysis_uploaded_marker_is_ascii_stable() -> None:
    assert analysis_candidate_has_already_uploaded([b"aLrEaDy UpLoAdEd"])
    assert not analysis_candidate_has_already_uploaded(["ＡLREADY UPLOADED".encode()])
    with pytest.raises(ByteDomainError, match="exact UTF-8"):
        analysis_candidate_has_already_uploaded([b"\xff"])


def test_artifact_component_codecs_match_independent_hardcoded_goldens() -> None:
    one = bytes.fromhex("01" * 32)
    two = bytes.fromhex("02" * 32)
    three = bytes.fromhex("03" * 32)
    four = bytes.fromhex("04" * 32)
    five = bytes.fromhex("05" * 32)
    six = bytes.fromhex("06" * 32)

    source = encode_artifact_source_manifest(one, 1, 2)
    effective = encode_artifact_effective_content([two, one, two])
    assert (
        b"".join(iter_artifact_effective_content_payload_ordered(3, [one, two, two]))
        == effective
    )
    assert artifact_effective_content_digest_ordered(3, iter([one, two, two])) == (
        artifact_effective_content_digest([two, one, two])
    )
    selected = encode_artifact_selected(one, two)
    owner = encode_artifact_owner(one, two, 7, three)
    policy = encode_artifact_policy(1, 2048, three)
    semantics = encode_artifact_semantics(one, two, three, four, five, six)
    comment = encode_zip_comment(one, three)

    assert ARTIFACT_COMPONENT_CODEC_VERSION == 1
    assert ARTIFACT_POLICY_CODEC_VERSION == 2
    assert artifact_source_manifest_digest(one, 1, 2).hex() == (
        "ba10d8d66e6eae463d8a23bf1547d16de02cedecdf03b3e76e4334cb736cf964"
    )
    assert artifact_effective_content_digest([two, one, two]).hex() == (
        "668d3f36923edde19a42ee69b207c8d136950b86144b1a2b5fff5995789e0144"
    )
    assert artifact_selected_digest(one, two).hex() == (
        "daa161fdd7112e9e73c7ab3c27c94e5dd871b77fc38d2701c0e680a1a11d0281"
    )
    assert artifact_owner_digest(one, two, 7, three).hex() == (
        "32d8d54e00e421fd40af6c8ff6e5849dcefabe8bcfaa067dd16ba337677dd908"
    )
    assert artifact_policy_digest(1, 2048, three).hex() == (
        "055021f55a25bb338b14aa4423b3fee9f8f87ff9ea442e4283ae89db88f47a60"
    )
    assert artifact_semantics_digest(one, two, three, four, five, six).hex() == (
        "24e1140357d6956ded50b48db8ee90171c7eff0b1179c4cf3636cfaf3dda2047"
    )
    assert hashlib.sha256(comment).hexdigest() == (
        "3acf99d73b12b308c807b543d62d43941cf8a530b0fadfc915bf735d614b59d0"
    )
    assert source.startswith(b"h2hdb-vnext-artifact-source-manifest\0")
    assert effective.startswith(b"h2hdb-vnext-artifact-effective-content\0")
    assert selected.endswith(one + two)
    assert owner.endswith(one + two + (7).to_bytes(8, "big") + three)
    assert policy.endswith((1).to_bytes(4, "big") + (2048).to_bytes(4, "big") + three)
    assert semantics.endswith(one + two + three + four + five + six)
    assert comment == (b"H2HDB-ZIP-COMMENT\0" + (1).to_bytes(4, "big") + one + three)


def test_artifact_semantic_codecs_exclude_noninjective_audit_inputs() -> None:
    assert "item_sha256" not in inspect.signature(encode_artifact_selected).parameters
    owner_parameters = inspect.signature(encode_artifact_owner).parameters
    assert "owner_decision_sha256" not in owner_parameters
    assert "winner_decision_sha256" not in owner_parameters
    source_parameters = inspect.signature(encode_artifact_source_manifest).parameters
    assert "build_manifest_sha256" not in source_parameters
    assert "gallery_manifest_sha256" not in source_parameters

    digest = bytes(32)
    with pytest.raises(DigestFormatError):
        encode_artifact_selected(bytes(31), digest)
    with pytest.raises(IntegerDomainError):
        encode_artifact_owner(digest, digest, 0, digest)
    with pytest.raises(IntegerDomainError):
        encode_artifact_policy(1, 0, digest)
    with pytest.raises(IntegerDomainError):
        encode_zip_comment(digest, digest, codec_version=2)


def test_artifact_producer_fingerprint_matches_independent_golden() -> None:
    values = (b"writer", b"cp314", b"pillow", b"libjpeg", b"zlib")
    payload = encode_artifact_producer_fingerprint(*values)

    assert ARTIFACT_PRODUCER_FINGERPRINT_CODEC_VERSION == 1
    assert payload.hex() == (
        "68326864622d766e6578742d61727469666163742d70726f647563657200"
        "00000001000000067772697465720000000563703331340000000670696c6c6f"
        "77000000076c69626a706567000000047a6c6962"
    )
    assert artifact_producer_fingerprint_sha256(*values).hex() == (
        "7c12521923b06e72b031807d2d2d82b5bee38afafd408595b5d29ed31cfe892c"
    )

    with pytest.raises(ByteDomainError, match="must not be empty"):
        encode_artifact_producer_fingerprint(b"", *values[1:])
    with pytest.raises(IntegerDomainError, match="not registered"):
        encode_artifact_producer_fingerprint(*values, codec_version=2)


def test_artifact_names_and_archive_members_are_fully_derived() -> None:
    assert artifact_name(7).hex() == "6832682d372e63627a"
    assert artifact_name((1 << 63) - 1) == b"h2h-9223372036854775807.cbz"
    assert decode_artifact_name(b"h2h-7.cbz") == 7
    assert decode_artifact_name(artifact_name((1 << 63) - 1)) == (1 << 63) - 1
    metadata_name = artifact_archive_member_name(
        0,
        ArtifactSourceRole.METADATA,
        ArtifactTransformKind.RAW_COPY,
        False,
    )
    assert metadata_name is not None
    assert metadata_name.hex() == (
        "303030303030303030303030303030305f5f6d657461646174612e747874"
    )
    content_name = artifact_archive_member_name(
        3,
        ArtifactSourceRole.CONTENT,
        ArtifactTransformKind.JPEG_NORMALIZE,
        False,
    )
    assert content_name is not None
    assert content_name.hex() == (
        "303030303030303030303030303030335f5f636f6e74656e742e6a7067"
    )
    assert (
        artifact_archive_member_name(
            9,
            ArtifactSourceRole.CONTENT,
            ArtifactTransformKind.GIF_NORMALIZE,
            True,
        )
        is None
    )

    for invalid_gid in (0, -1, 1 << 63, True):
        with pytest.raises(IntegerDomainError):
            artifact_name(invalid_gid)
    with pytest.raises(ByteDomainError, match="METADATA.*RAW_COPY"):
        artifact_archive_member_name(
            0,
            ArtifactSourceRole.METADATA,
            ArtifactTransformKind.JPEG_NORMALIZE,
            True,
        )


def test_artifact_protection_token_matches_fixed_184_byte_golden() -> None:
    candidate_id = bytes.fromhex("11" * 16)
    publication_key_value = bytes.fromhex("22" * 32)
    artifact_sha256 = bytes.fromhex("33" * 32)
    locator_sha256 = bytes.fromhex("44" * 32)
    receipt_id = artifact_storage_receipt_id(
        candidate_id,
        publication_key_value,
        artifact_sha256,
        locator_sha256,
        7,
        9,
    )
    token = encode_artifact_protection_token(
        1,
        candidate_id,
        publication_key_value,
        artifact_sha256,
        locator_sha256,
        7,
        9,
    )

    assert ARTIFACT_PROTECTION_TOKEN_CODEC_VERSION == 1
    assert receipt_id.hex() == "be24a65b2ded7965b31c3c317bc61cbf"
    assert len(token) == 184
    assert token.hex() == (
        "68326864622d766e6578742d61727469666163742d70726f74656374696f6e00"
        "0000000100000001111111111111111111111111111111112222222222222222"
        "2222222222222222222222222222222222222222222222223333333333333333"
        "3333333333333333333333333333333333333333333333334444444444444444"
        "444444444444444444444444444444444444444444444444be24a65b2ded7965"
        "b31c3c317bc61cbf00000000000000070000000000000009"
    )
    assert decode_artifact_protection_token(token) == ArtifactProtectionToken(
        codec_version=1,
        storage_codec_version=1,
        candidate_id=candidate_id,
        publication_key=publication_key_value,
        artifact_sha256=artifact_sha256,
        artifact_storage_key_sha256=locator_sha256,
        receipt_id=receipt_id,
        storage_generation=7,
        size_bytes=9,
    )

    for malformed in (token[:-1], token + b"x"):
        with pytest.raises(ByteDomainError, match="exactly 184 bytes"):
            decode_artifact_protection_token(malformed)
    corrupt_prefix = bytearray(token)
    corrupt_prefix[0] ^= 1
    with pytest.raises(ByteDomainError, match="prefix"):
        decode_artifact_protection_token(bytes(corrupt_prefix))
    corrupt_version = bytearray(token)
    corrupt_version[35] = 2
    with pytest.raises(IntegerDomainError, match="not registered"):
        decode_artifact_protection_token(bytes(corrupt_version))
    corrupt_receipt = bytearray(token)
    corrupt_receipt[152] ^= 1
    with pytest.raises(DigestMismatchError, match="receipt_id"):
        decode_artifact_protection_token(bytes(corrupt_receipt))


def _source_snapshot_fixture() -> tuple[
    SourceSnapshotPolicy,
    SourceSnapshotCounts,
    tuple[SourceSnapshotGallery, ...],
    tuple[SourceSnapshotFileHashDecision, ...],
    tuple[SourceSnapshotContentOwner, ...],
    tuple[SourceSnapshotGidWinner, ...],
]:
    policy = SourceSnapshotPolicy(
        analysis_algorithm_version=1,
        spam_artist_threshold=2,
        spam_occurrence_threshold=3,
        content_owner_rule_version=1,
        gid_winner_rule_version=1,
    )
    counts = SourceSnapshotCounts(gallery_count=2, file_count=3, byte_count=300)
    # Deliberately reverse every repeated section's canonical order.
    galleries = (
        SourceSnapshotGallery(
            gallery_key=bytes.fromhex("10" * 32),
            observation_identity_sha256=bytes.fromhex("20" * 32),
            content_sha256=None,
            gid=5,
            file_count=1,
            byte_count=100,
        ),
        SourceSnapshotGallery(
            gallery_key=bytes.fromhex("01" * 32),
            observation_identity_sha256=bytes.fromhex("02" * 32),
            content_sha256=bytes.fromhex("03" * 32),
            gid=7,
            file_count=2,
            byte_count=200,
        ),
    )
    decisions = (
        SourceSnapshotFileHashDecision(
            file_sha256=bytes.fromhex("aa" * 32),
            occurrence_count=3,
            artist_count=5,
            maximum_gallery_artist_count=2,
            excluded_flag=True,
        ),
        SourceSnapshotFileHashDecision(
            file_sha256=bytes(32),
            occurrence_count=2,
            artist_count=100,
            maximum_gallery_artist_count=1,
            excluded_flag=False,
        ),
    )
    owners = (
        SourceSnapshotContentOwner(
            content_sha256=bytes.fromhex("03" * 32),
            owner_gallery_key=bytes.fromhex("01" * 32),
        ),
    )
    winners = (
        SourceSnapshotGidWinner(
            gid=7,
            winner_gallery_key=bytes.fromhex("01" * 32),
        ),
        SourceSnapshotGidWinner(
            gid=5,
            winner_gallery_key=bytes.fromhex("10" * 32),
        ),
    )
    return policy, counts, galleries, decisions, owners, winners


def test_source_snapshot_manifest_matches_independent_hardcoded_golden() -> None:
    policy, counts, galleries, decisions, owners, winners = _source_snapshot_fixture()

    def u32(value: int) -> bytes:
        return value.to_bytes(4, "big")

    def u64(value: int) -> bytes:
        return value.to_bytes(8, "big")

    expected_payload = (
        b"h2hdb-vnext-source-snapshot-manifest\0"
        + u32(1)
        + u32(1)
        + u64(2)
        + u64(3)
        + u32(1)
        + u32(1)
        + u64(2)
        + u64(3)
        + u64(300)
        + u64(2)
        + bytes.fromhex("01" * 32)
        + bytes.fromhex("02" * 32)
        + b"\x01"
        + bytes.fromhex("03" * 32)
        + u64(7)
        + bytes.fromhex("10" * 32)
        + bytes.fromhex("20" * 32)
        + b"\x00"
        + u64(5)
        + u64(2)
        + bytes(32)
        + u64(2)
        + u64(100)
        + u64(1)
        + b"\x00"
        + bytes.fromhex("aa" * 32)
        + u64(3)
        + u64(5)
        + u64(2)
        + b"\x01"
        + u64(1)
        + bytes.fromhex("03" * 32)
        + bytes.fromhex("01" * 32)
        + u64(2)
        + u64(5)
        + bytes.fromhex("10" * 32)
        + u64(7)
        + bytes.fromhex("01" * 32)
    )

    actual = encode_source_snapshot_manifest(
        policy,
        counts,
        galleries,
        decisions,
        owners,
        winners,
    )

    assert SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION == 1
    assert actual == expected_payload
    ordered_galleries = tuple(sorted(galleries, key=lambda row: row.gallery_key))
    ordered_decisions = tuple(sorted(decisions, key=lambda row: row.file_sha256))
    ordered_owners = tuple(sorted(owners, key=lambda row: row.content_sha256))
    ordered_winners = tuple(sorted(winners, key=lambda row: row.gid))
    assert (
        b"".join(
            iter_source_snapshot_manifest_payload_ordered(
                policy,
                counts,
                ordered_galleries,
                ordered_decisions,
                ordered_owners,
                ordered_winners,
            )
        )
        == expected_payload
    )
    assert (
        b"".join(
            iter_source_snapshot_manifest_payload_rows_ordered(
                policy,
                counts,
                2,
                iter(ordered_galleries),
                2,
                iter(ordered_decisions),
                1,
                iter(ordered_owners),
                2,
                iter(ordered_winners),
            )
        )
        == expected_payload
    )
    assert len(actual) == 561
    assert (
        source_snapshot_manifest_digest(
            policy,
            counts,
            galleries,
            decisions,
            owners,
            winners,
        ).hex()
        == "8e7262dc0d3147a70827b2a6188354b2b026a9fb2adfcaddc896acf0c5f9d2a8"
    )
    assert source_snapshot_manifest_digest_ordered(
        policy,
        counts,
        2,
        iter(ordered_galleries),
        2,
        iter(ordered_decisions),
        1,
        iter(ordered_owners),
        2,
        iter(ordered_winners),
        payload_byte_count=len(expected_payload),
    ) == source_snapshot_manifest_digest(
        policy,
        counts,
        galleries,
        decisions,
        owners,
        winners,
    )


def test_source_snapshot_manifest_canonicalizes_all_section_permutations() -> None:
    policy, counts, galleries, decisions, owners, winners = _source_snapshot_fixture()
    expected = encode_source_snapshot_manifest(
        policy,
        counts,
        galleries,
        decisions,
        owners,
        winners,
    )

    assert (
        encode_source_snapshot_manifest(
            policy,
            counts,
            tuple(reversed(galleries)),
            tuple(reversed(decisions)),
            tuple(reversed(owners)),
            tuple(reversed(winners)),
        )
        == expected
    )


def test_ordered_snapshot_stream_fails_closed_on_receipt_drift() -> None:
    policy, counts, galleries, decisions, owners, winners = _source_snapshot_fixture()
    ordered_galleries = tuple(sorted(galleries, key=lambda row: row.gallery_key))
    ordered_decisions = tuple(sorted(decisions, key=lambda row: row.file_sha256))
    ordered_winners = tuple(sorted(winners, key=lambda row: row.gid))
    with pytest.raises(ByteDomainError, match="gallery_entry_count"):
        source_snapshot_manifest_digest_ordered(
            policy,
            counts,
            1,
            iter(ordered_galleries),
            2,
            iter(ordered_decisions),
            1,
            iter(owners),
            2,
            iter(ordered_winners),
            payload_byte_count=561,
        )
    with pytest.raises(ByteDomainError, match="byte_count"):
        source_snapshot_manifest_digest_ordered(
            policy,
            counts,
            2,
            iter(ordered_galleries),
            2,
            iter(ordered_decisions),
            1,
            iter(owners),
            2,
            iter(ordered_winners),
            payload_byte_count=560,
        )


def test_source_snapshot_optional_content_distinguishes_none_from_zero_digest() -> None:
    policy = SourceSnapshotPolicy(1, 1, 1, 1, 1)
    counts = SourceSnapshotCounts(1, 0, 0)
    gallery_key = bytes.fromhex("11" * 32)
    observation = bytes.fromhex("22" * 32)
    no_content_gallery = SourceSnapshotGallery(
        gallery_key,
        observation,
        None,
        9,
        0,
        0,
    )
    zero_content_gallery = SourceSnapshotGallery(
        gallery_key,
        observation,
        bytes(32),
        9,
        0,
        0,
    )
    winner = SourceSnapshotGidWinner(9, gallery_key)

    metadata_only = encode_source_snapshot_manifest(
        policy,
        counts,
        [no_content_gallery],
        [],
        [],
        [winner],
    )
    zero_digest = encode_source_snapshot_manifest(
        policy,
        counts,
        [zero_content_gallery],
        [],
        [SourceSnapshotContentOwner(bytes(32), gallery_key)],
        [winner],
    )

    assert metadata_only != zero_digest
    assert source_snapshot_manifest_digest(
        policy, counts, [no_content_gallery], [], [], [winner]
    ) != source_snapshot_manifest_digest(
        policy,
        counts,
        [zero_content_gallery],
        [],
        [SourceSnapshotContentOwner(bytes(32), gallery_key)],
        [winner],
    )


def test_source_snapshot_identity_includes_exact_counts_but_no_audit_digests() -> None:
    policy, counts, galleries, decisions, owners, winners = _source_snapshot_fixture()
    original = source_snapshot_manifest_digest(
        policy, counts, galleries, decisions, owners, winners
    )
    changed_decisions = (
        decisions[0],
        SourceSnapshotFileHashDecision(
            file_sha256=decisions[1].file_sha256,
            occurrence_count=decisions[1].occurrence_count,
            artist_count=99,
            maximum_gallery_artist_count=decisions[1].maximum_gallery_artist_count,
            excluded_flag=False,
        ),
    )

    assert (
        source_snapshot_manifest_digest(
            policy,
            counts,
            galleries,
            changed_decisions,
            owners,
            winners,
        )
        != original
    )
    assert "evidence_sha256" not in {
        item.name for item in fields(SourceSnapshotFileHashDecision)
    }
    assert "decision_sha256" not in {
        item.name for item in fields(SourceSnapshotContentOwner)
    }
    assert "decision_sha256" not in {
        item.name for item in fields(SourceSnapshotGidWinner)
    }


@pytest.mark.parametrize(
    "decision",
    [
        SourceSnapshotFileHashDecision(bytes(32), 3, 5, 2, False),
        SourceSnapshotFileHashDecision(bytes(32), 2, 100, 1, True),
    ],
)
def test_source_snapshot_rejects_supplied_exclusion_predicate_mismatch(
    decision: SourceSnapshotFileHashDecision,
) -> None:
    policy, counts, galleries, _, owners, winners = _source_snapshot_fixture()

    with pytest.raises(ByteDomainError, match="predicate"):
        encode_source_snapshot_manifest(
            policy,
            counts,
            galleries,
            [decision],
            owners,
            winners,
        )


def test_source_snapshot_spam_product_uses_unbounded_integers() -> None:
    maximum = (1 << 63) - 1
    policy = SourceSnapshotPolicy(1, maximum, 0, 1, 1)
    decision = SourceSnapshotFileHashDecision(
        bytes(32),
        0,
        maximum,
        maximum,
        False,
    )

    encode_source_snapshot_manifest(
        policy,
        SourceSnapshotCounts(0, 0, 0),
        [],
        [decision],
        [],
        [],
    )


def test_source_snapshot_rejects_declared_aggregate_mismatch() -> None:
    policy, _, galleries, decisions, owners, winners = _source_snapshot_fixture()

    with pytest.raises(ByteDomainError, match="do not match"):
        encode_source_snapshot_manifest(
            policy,
            SourceSnapshotCounts(2, 4, 300),
            galleries,
            decisions,
            owners,
            winners,
        )


def test_source_snapshot_rejects_owner_and_winner_outside_snapshot() -> None:
    policy, counts, galleries, decisions, owners, winners = _source_snapshot_fixture()
    outsider = bytes.fromhex("ff" * 32)

    with pytest.raises(ByteDomainError, match="owner gallery is not"):
        encode_source_snapshot_manifest(
            policy,
            counts,
            galleries,
            decisions,
            [SourceSnapshotContentOwner(owners[0].content_sha256, outsider)],
            winners,
        )
    with pytest.raises(ByteDomainError, match="winner gallery is not"):
        encode_source_snapshot_manifest(
            policy,
            counts,
            galleries,
            decisions,
            owners,
            [winners[0], SourceSnapshotGidWinner(5, outsider)],
        )


def test_source_snapshot_rejects_owner_and_winner_group_mismatch() -> None:
    policy, counts, galleries, decisions, owners, winners = _source_snapshot_fixture()

    with pytest.raises(ByteDomainError, match="content group"):
        encode_source_snapshot_manifest(
            policy,
            counts,
            galleries,
            decisions,
            [
                SourceSnapshotContentOwner(
                    owners[0].content_sha256,
                    galleries[0].gallery_key,
                )
            ],
            winners,
        )
    with pytest.raises(ByteDomainError, match="GID group"):
        encode_source_snapshot_manifest(
            policy,
            counts,
            galleries,
            decisions,
            owners,
            [
                SourceSnapshotGidWinner(5, galleries[1].gallery_key),
                winners[0],
            ],
        )


def test_source_snapshot_rejects_duplicate_keys_in_every_section() -> None:
    policy, counts, galleries, decisions, owners, winners = _source_snapshot_fixture()
    cases = (
        ([galleries[0], galleries[0]], decisions, owners, winners),
        (galleries, [decisions[0], decisions[0]], owners, winners),
        (galleries, decisions, [owners[0], owners[0]], winners),
        (galleries, decisions, owners, [winners[0], winners[0], winners[1]]),
    )

    for gallery_rows, decision_rows, owner_rows, winner_rows in cases:
        with pytest.raises(ByteDomainError, match="duplicate"):
            encode_source_snapshot_manifest(
                policy,
                counts,
                gallery_rows,
                decision_rows,
                owner_rows,
                winner_rows,
            )


def test_gallery_key_matches_independent_golden_framing() -> None:
    scope_key = bytes(range(32))
    locator_sha256 = source_relative_locator_digest(
        "source_relative_locator_v1", ("nested", "畫廊 A")
    )

    actual = gallery_key(scope_key, locator_sha256)

    assert (
        actual.hex()
        == "7e5c25a4144d31c0e6cfc3fc5380b1b65659356ca1d1602dea4503c45485975f"
    )
    assert actual == _manual_gallery_key(1, scope_key, locator_sha256)
    assert gallery_key_hex(scope_key, locator_sha256) == actual.hex()


def test_gallery_key_carries_scope_and_full_locator_identity() -> None:
    scope_a = bytes(32)
    scope_b = bytes(31) + b"\x01"
    locator_a = source_relative_locator_digest(
        "source_relative_locator_v1", ("parent-a", "same")
    )
    locator_b = source_relative_locator_digest(
        "source_relative_locator_v1", ("parent-b", "same")
    )

    assert gallery_key(scope_a, locator_a) != gallery_key(scope_b, locator_a)
    assert gallery_key(scope_a, locator_a) != gallery_key(scope_a, locator_b)

    with pytest.raises(ByteDomainError):
        gallery_key(bytes(31), locator_a)
    with pytest.raises(ByteDomainError):
        gallery_key(bytes(33), locator_a)
    with pytest.raises(ByteDomainError):
        gallery_key(scope_a, bytes(31))


def test_source_scope_key_matches_independent_framing_and_all_inputs() -> None:
    provider = b"filesystem"
    root_sha256 = hashlib.sha256(b"/exact/source/root").digest()
    expected = bytes.fromhex(
        "56c9fd388b9c592ffc9f43c3d5ea38735ccd5293a5ab9bcc541a7336362dacd2"
    )

    actual = source_scope_key("filesystem", root_sha256, 9)

    manual = hashlib.sha256(
        b"h2hdb-vnext-source-scope-key\0"
        + (1).to_bytes(4, "big")
        + len(provider).to_bytes(4, "big")
        + provider
        + root_sha256
        + (9).to_bytes(4, "big")
    ).digest()
    assert actual == expected
    assert actual == manual
    assert actual != source_scope_key("filesystem", root_sha256, 10)
    assert actual != source_scope_key("filesystem", bytes(32), 9)
    assert SOURCE_PROVIDERS == {"filesystem"}
    with pytest.raises(RegisteredIdentifierError):
        source_scope_key("another", root_sha256, 9)


def test_file_key_and_role_are_exact_versioned_contracts() -> None:
    name = b"galleryinfo.txt"
    expected = hashlib.sha256(
        b"h2hdb-vnext-file-key\0"
        + (1).to_bytes(4, "big")
        + len(name).to_bytes(4, "big")
        + name
    ).digest()

    assert file_key(name) == expected
    assert file_key(b"A.jpg") != file_key(b"a.jpg")
    assert file_role(name) == b"METADATA"
    assert file_role(b"GalleryInfo.txt") == b"CONTENT"
    assert file_role(b"001.jpg") == b"CONTENT"

    with pytest.raises(IntegerDomainError):
        file_key(name, algorithm_version=2)
    with pytest.raises(IntegerDomainError):
        file_role(name, classifier_version=2)


def _golden_member_plan_entries() -> tuple[ArtifactMemberPlanEntry, ...]:
    return (
        ArtifactMemberPlanEntry(
            entry_position=0,
            source_name_bytes=b"galleryinfo.txt",
            source_file_sha256=bytes(range(32)),
            source_size_bytes=123,
            excluded_flag=False,
        ),
        ArtifactMemberPlanEntry(
            entry_position=1,
            source_name_bytes=b"IMAGE.GIF",
            source_file_sha256=bytes(range(255, 223, -1)),
            source_size_bytes=0,
            excluded_flag=True,
        ),
    )


def test_artifact_member_plan_matches_hardcoded_closed_tag_golden() -> None:
    entries = _golden_member_plan_entries()
    expected_payload = bytes.fromhex(
        "68326864622d766e6578742d61727469666163742d6d656d6265722d706c616e00000000"
        "0100000000000000020000000000000000000000000f67616c6c657279696e666f2e7478"
        "74000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f000000"
        "000000007b0000010000001e303030303030303030303030303030305f5f6d6574616461"
        "74612e7478740000000000000000010000000009494d4147452e474946fffefdfcfbfaf9"
        "f8f7f6f5f4f3f2f1f0efeeedecebeae9e8e7e6e5e4e3e2e1e00000000000000000010100"
        "01"
    )

    actual = encode_artifact_member_plan(entries)

    assert ARTIFACT_MEMBER_PLAN_VERSION == 1
    assert ArtifactMemberEntryKind.__members__ == {"SOURCE_FILE": 0}
    assert ArtifactSourceRole.__members__ == {"METADATA": 0, "CONTENT": 1}
    assert ArtifactTransformKind.__members__ == {
        "RAW_COPY": 0,
        "GIF_NORMALIZE": 1,
        "JPEG_NORMALIZE": 2,
    }
    assert actual == expected_payload
    assert b"".join(iter_artifact_member_plan_payload(2, entries)) == expected_payload
    assert artifact_member_plan_digest_ordered(
        2,
        len(expected_payload),
        iter(entries),
    ) == artifact_member_plan_digest(entries)
    assert decode_artifact_member_plan(actual) == entries
    assert artifact_member_plan_digest(entries).hex() == (
        "783a1b7b319bedd73edf61afa00cc9cd419ae34e9b85fe8f4c39bfae7c13f690"
    )
    assert b"H2HDB-ZIP-COMMENT" not in actual


@pytest.mark.parametrize(
    ("name", "role", "transform"),
    [
        (
            b"galleryinfo.txt",
            ArtifactSourceRole.METADATA,
            ArtifactTransformKind.RAW_COPY,
        ),
        (
            b"GalleryInfo.txt",
            ArtifactSourceRole.CONTENT,
            ArtifactTransformKind.RAW_COPY,
        ),
        (b"image.GiF", ArtifactSourceRole.CONTENT, ArtifactTransformKind.GIF_NORMALIZE),
        (
            b"image.AvIf",
            ArtifactSourceRole.CONTENT,
            ArtifactTransformKind.JPEG_NORMALIZE,
        ),
        (
            b"image.BMP",
            ArtifactSourceRole.CONTENT,
            ArtifactTransformKind.JPEG_NORMALIZE,
        ),
        (
            b"image.JpEg",
            ArtifactSourceRole.CONTENT,
            ArtifactTransformKind.JPEG_NORMALIZE,
        ),
        (
            b"image.JPG",
            ArtifactSourceRole.CONTENT,
            ArtifactTransformKind.JPEG_NORMALIZE,
        ),
        (
            b"image.PnG",
            ArtifactSourceRole.CONTENT,
            ArtifactTransformKind.JPEG_NORMALIZE,
        ),
        (
            b"image.WebP",
            ArtifactSourceRole.CONTENT,
            ArtifactTransformKind.JPEG_NORMALIZE,
        ),
        (b"image.tiff", ArtifactSourceRole.CONTENT, ArtifactTransformKind.RAW_COPY),
        (b"image.\xffGIF", ArtifactSourceRole.CONTENT, ArtifactTransformKind.RAW_COPY),
    ],
)
def test_artifact_member_plan_derives_role_and_transform_from_exact_name(
    name: bytes,
    role: ArtifactSourceRole,
    transform: ArtifactTransformKind,
) -> None:
    entry = ArtifactMemberPlanEntry(
        entry_position=0,
        source_name_bytes=name,
        source_file_sha256=bytes(32),
        source_size_bytes=0,
        excluded_flag=False,
    )

    assert entry.entry_kind is ArtifactMemberEntryKind.SOURCE_FILE
    assert entry.source_role is role
    assert entry.transform_kind is transform
    assert entry.archive_member_name_bytes == artifact_archive_member_name(
        entry.entry_position,
        role,
        transform,
        False,
    )
    assert decode_artifact_member_plan(encode_artifact_member_plan([entry])) == (entry,)


def test_artifact_member_plan_rejects_noncontiguous_positions() -> None:
    first = _golden_member_plan_entries()[0]
    gap = ArtifactMemberPlanEntry(
        entry_position=2,
        source_name_bytes=b"002.jpg",
        source_file_sha256=bytes.fromhex("22" * 32),
        source_size_bytes=1,
        excluded_flag=False,
    )

    with pytest.raises(ByteDomainError, match="contiguous"):
        encode_artifact_member_plan([first, gap])

    with pytest.raises(ByteDomainError, match="declared_byte_count"):
        artifact_member_plan_digest_ordered(
            1,
            len(encode_artifact_member_plan([first])) + 1,
            iter((first,)),
        )


def test_artifact_member_plan_rejects_impossible_presence_and_domains() -> None:
    with pytest.raises(ByteDomainError, match="exactly bool"):
        ArtifactMemberPlanEntry(
            entry_position=0,
            source_name_bytes=b"001.jpg",
            source_file_sha256=bytes(32),
            source_size_bytes=1,
            excluded_flag=1,  # type: ignore[arg-type]
        )
    with pytest.raises(DigestFormatError):
        ArtifactMemberPlanEntry(
            entry_position=0,
            source_name_bytes=b"001.jpg",
            source_file_sha256=bytes(31),
            source_size_bytes=1,
            excluded_flag=False,
        )
    with pytest.raises(IntegerDomainError):
        ArtifactMemberPlanEntry(
            entry_position=0,
            source_name_bytes=b"001.jpg",
            source_file_sha256=bytes(32),
            source_size_bytes=-1,
            excluded_flag=False,
        )


@pytest.mark.parametrize(
    ("offset", "replacement"),
    [
        (53, 1),  # generated/unknown entry kind
        (113, 9),  # unknown role
        (114, 2),  # non-boolean exclusion
        (115, 2),  # non-boolean presence
        (150, 9),  # unknown transform
        (113, 1),  # registered role inconsistent with galleryinfo.txt
        (150, 1),  # registered transform inconsistent with .txt
        (158, 2),  # noncontiguous second position
    ],
)
def test_artifact_member_plan_decoder_rejects_unknown_or_inconsistent_tags(
    offset: int,
    replacement: int,
) -> None:
    malformed = bytearray(encode_artifact_member_plan(_golden_member_plan_entries()))
    malformed[offset] = replacement

    with pytest.raises(ByteDomainError):
        decode_artifact_member_plan(bytes(malformed))


def test_artifact_member_plan_decoder_rejects_trailing_truncated_and_version() -> None:
    payload = encode_artifact_member_plan(_golden_member_plan_entries())

    with pytest.raises(ByteDomainError, match="trailing"):
        decode_artifact_member_plan(payload + b"\x00")
    with pytest.raises(ByteDomainError, match="truncated"):
        decode_artifact_member_plan(payload[:-1])

    unknown_version = bytearray(payload)
    unknown_version[36] = 2
    with pytest.raises(IntegerDomainError, match="not registered"):
        decode_artifact_member_plan(bytes(unknown_version))

    with pytest.raises(IntegerDomainError):
        encode_artifact_member_plan([], plan_version=2)


def test_leaf_byte_boundaries_preserve_exact_bytes() -> None:
    max_utf8 = "é" * 127 + "a"
    max_opaque = b"\xff" * 255

    assert len(validate_gallery_name(max_utf8)) == 255
    assert validate_file_name(max_opaque) is max_opaque
    assert validate_namespace("é" * 64) == ("é" * 64).encode()

    with pytest.raises(ByteDomainError):
        validate_gallery_name("é" * 128)
    with pytest.raises(ByteDomainError):
        validate_file_name(max_opaque + b"x")
    with pytest.raises(ByteDomainError):
        validate_namespace("é" * 65)


@pytest.mark.parametrize("value", ["", "\x00", ".", "..", "a/b", "a\\b"])
@pytest.mark.parametrize("validator", [validate_gallery_name])
def test_utf8_leaf_domains_reject_non_leaf_values(
    value: str, validator: object
) -> None:
    with pytest.raises(ByteDomainError):
        validator(value)  # type: ignore[operator]


@pytest.mark.parametrize("value", [b"", b"\x00", b".", b"..", b"a/b"])
@pytest.mark.parametrize("validator", [validate_file_name])
def test_opaque_leaf_domains_reject_non_leaf_values(
    value: bytes, validator: object
) -> None:
    with pytest.raises(ByteDomainError):
        validator(value)  # type: ignore[operator]


def test_posix_opaque_file_name_allows_backslash() -> None:
    assert validate_file_name(b"a\\b") == b"a\\b"


@pytest.mark.parametrize("validator", [validate_gallery_name, validate_namespace])
def test_utf8_domains_reject_surrogates(validator: object) -> None:
    with pytest.raises(ByteDomainError):
        validator("bad\ud800name")  # type: ignore[operator]


def test_namespace_is_exact_utf8_not_a_filesystem_leaf_domain() -> None:
    for value in ("", "\x00", ".", "..", "a/b", "a\\b", " A ", "藝術家"):
        assert validate_namespace(value) == value.encode("utf-8")

    assert validate_namespace("A") != validate_namespace("a")
    assert validate_namespace("A") != validate_namespace("A ")


def test_nested_source_locator_matches_golden_framing_and_round_trips() -> None:
    components = ("parent", "子目錄", "same")
    encoded = encode_source_relative_locator(components)

    assert encoded == _manual_locator(components)
    assert decode_source_relative_locator(encoded) == components
    assert source_relative_locator_digest(
        "source_relative_locator_v1", components
    ) == canonical_value_digest("source_relative_locator_v1", encoded)
    assert b"".join(iter_source_relative_locator_payload(components)) == encoded
    for split in range(len(encoded) + 1):
        receipt = validate_source_relative_locator_parts(
            (encoded[:split], encoded[split:])
        )
        assert receipt == SourceRelativeLocatorValidationReceipt(
            len(components), len(encoded), hashlib.sha256(encoded).digest()
        )

    with pytest.raises(ByteDomainError, match="truncated"):
        validate_source_relative_locator_parts((encoded[:-1],))
    with pytest.raises(ByteDomainError, match="trailing"):
        validate_source_relative_locator_parts((encoded, b"x"))


def test_nested_source_locator_total_length_is_not_silently_truncated() -> None:
    components = tuple(f"segment-{index:04}" for index in range(512))

    encoded = encode_source_relative_locator(components)

    assert len(encoded) > 4096
    assert decode_source_relative_locator(encoded) == components


@pytest.mark.parametrize(
    "components",
    [(), ("",), (".",), ("..",), ("a/b",), ("a\\b",), ("bad\x00name",)],
)
def test_nested_source_locator_rejects_unsafe_components(
    components: tuple[str, ...],
) -> None:
    with pytest.raises(ByteDomainError):
        encode_source_relative_locator(components)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\x00" * 7,
        (2).to_bytes(4, "big") + (1).to_bytes(4, "big") + b"\x00\x00\x00\x01a",
        (1).to_bytes(4, "big") + (0).to_bytes(4, "big"),
        _manual_locator(("a",)) + b"trailing",
        (1).to_bytes(4, "big") + (1).to_bytes(4, "big") + (3).to_bytes(4, "big") + b"a",
    ],
)
def test_nested_source_locator_rejects_malformed_payload(payload: bytes) -> None:
    with pytest.raises((ByteDomainError, IntegerDomainError)):
        decode_source_relative_locator(payload)


def test_registered_ascii_identifiers_are_exact_allowed_members() -> None:
    allowed = frozenset({"DISCOVER", "FINAL_ANALYSES"})

    assert (
        validate_registered_ascii_identifier(
            "DISCOVER", allowed=allowed, field_name="stage", maximum_bytes=64
        )
        == b"DISCOVER"
    )
    with pytest.raises(RegisteredIdentifierError):
        validate_registered_ascii_identifier(
            "discover", allowed=allowed, field_name="stage", maximum_bytes=64
        )
    with pytest.raises(RegisteredIdentifierError):
        validate_registered_ascii_identifier(
            "DISCOVER ", allowed=allowed, field_name="stage", maximum_bytes=64
        )
    with pytest.raises(RegisteredIdentifierError):
        validate_registered_ascii_identifier(
            "非ASCII", allowed={"非ASCII"}, field_name="stage", maximum_bytes=64
        )
    with pytest.raises(RegisteredIdentifierError):
        validate_registered_ascii_identifier(
            "DISCOVER",
            allowed={"DISCOVER", "非ASCII"},
            field_name="stage",
            maximum_bytes=64,
        )


def test_contract_owned_component_registries_are_closed() -> None:
    assert ANALYSIS_STATE_COMPONENTS == {
        "file_hash_decision",
        "content_owner_candidate",
        "content_owner",
        "gid_candidate",
        "gid_winner",
    }
    assert ARTIFACT_COMPONENT_KINDS == {
        "source_manifest",
        "member_plan",
        "effective_content",
        "selected",
        "owner",
        "policy",
    }
    assert validate_state_component("gid_winner") == b"gid_winner"
    assert validate_artifact_component_kind("effective_content") == b"effective_content"

    with pytest.raises(RegisteredIdentifierError):
        validate_state_component("future_component")
    with pytest.raises(RegisteredIdentifierError):
        validate_artifact_component_kind("content_sha256")


def test_generated_publication_and_artifact_identifiers_are_exact() -> None:
    gid = (1 << 63) - 1
    artifact_digest = bytes.fromhex("ab" * 32)

    encoded_publication = publication_id(gid)
    binary_publication_key = publication_key(gid)
    encoded_artifact = artifact_id(gid, artifact_digest)

    assert encoded_publication == f"urn:h2h:gallery:{gid}".encode()
    assert decode_publication_id(encoded_publication) == gid
    assert decode_publication_id(publication_id(1)) == 1
    assert binary_publication_key.hex() == (
        "ef9a3bbaa67483f863e6aa50c1c8f2b97969a6acf1b21d6ea77df181e3bb0fd2"
    )
    assert (
        binary_publication_key
        == hashlib.sha256(
            b"h2hdb-vnext-publication-key\0"
            + (1).to_bytes(4, "big")
            + gid.to_bytes(8, "big")
        ).digest()
    )
    assert publication_key_hex(gid) == binary_publication_key.hex()
    assert encoded_artifact == (
        f"urn:h2h:artifact:cbz:{gid}:sha256:{'ab' * 32}".encode()
    )
    assert decode_artifact_id(encoded_artifact) == (gid, artifact_digest)
    assert decode_artifact_id(artifact_id(1, bytes(32))) == (1, bytes(32))
    assert len(encoded_publication) <= 64
    assert len(encoded_artifact) <= 128


@pytest.mark.parametrize(
    "gid_ascii",
    (
        b"",
        b"01",
        b"+1",
        b" 1",
        b"1 ",
        "\N{FULLWIDTH DIGIT ONE}".encode(),
        "\N{ARABIC-INDIC DIGIT ONE}".encode(),
    ),
)
def test_artifact_id_decoder_rejects_noncanonical_gid_text(
    gid_ascii: bytes,
) -> None:
    encoded = b"urn:h2h:artifact:cbz:" + gid_ascii + b":sha256:" + b"ab" * 32

    with pytest.raises(ByteDomainError, match="gid|leading zero"):
        decode_artifact_id(encoded)


@pytest.mark.parametrize("gid_ascii", (b"0", b"9223372036854775808"))
def test_artifact_id_decoder_rejects_gid_outside_positive_int63(
    gid_ascii: bytes,
) -> None:
    encoded = b"urn:h2h:artifact:cbz:" + gid_ascii + b":sha256:" + b"ab" * 32

    with pytest.raises(IntegerDomainError, match="artifact_id gid"):
        decode_artifact_id(encoded)


@pytest.mark.parametrize(
    "digest_ascii",
    (
        b"ab" * 31,
        b"ab" * 32 + b"0",
        b"AB" * 32,
        b"ag" * 32,
        b"ab" * 32 + b"junk",
        "\N{FULLWIDTH DIGIT ZERO}".encode() + b"0" * 61,
    ),
)
def test_artifact_id_decoder_rejects_noncanonical_digest_text(
    digest_ascii: bytes,
) -> None:
    encoded = b"urn:h2h:artifact:cbz:1:sha256:" + digest_ascii

    with pytest.raises(DigestFormatError, match="64 lowercase hex"):
        decode_artifact_id(encoded)


@pytest.mark.parametrize(
    "encoded",
    (
        b"",
        b"URN:h2h:artifact:cbz:1:sha256:" + b"ab" * 32,
        b"urn:h2h:artifact:zip:1:sha256:" + b"ab" * 32,
        b"urn:h2h:artifact:cbz:1:" + b"ab" * 32,
        b"urn:h2h:artifact:cbz:1sha256:" + b"ab" * 32,
    ),
)
def test_artifact_id_decoder_rejects_wrong_registered_structure(
    encoded: bytes,
) -> None:
    with pytest.raises(ByteDomainError, match="prefix|separator"):
        decode_artifact_id(encoded)


def test_artifact_id_decoder_requires_immutable_bytes_and_bounded_input() -> None:
    with pytest.raises(ByteDomainError, match="immutable bytes"):
        decode_artifact_id(bytearray(b"artifact"))  # type: ignore[arg-type]
    with pytest.raises(ByteDomainError, match="exceeds 128 bytes"):
        decode_artifact_id(b"urn:h2h:artifact:cbz:" + b"1" * 129)


def test_publication_id_and_artifact_name_decoders_round_trip_decimal_boundaries() -> (
    None
):
    gids = [1, (1 << 31) - 1, 1 << 31, (1 << 32) - 1, 1 << 32]
    for power in range(1, 19):
        gids.extend((10**power - 1, 10**power))
    gids.append((1 << 63) - 1)

    for gid in dict.fromkeys(gids):
        encoded_publication = publication_id(gid)
        encoded_name = artifact_name(gid)
        assert decode_publication_id(encoded_publication) == gid
        assert decode_artifact_name(encoded_name) == gid
        assert publication_id(decode_publication_id(encoded_publication)) == (
            encoded_publication
        )
        assert artifact_name(decode_artifact_name(encoded_name)) == encoded_name

    assert len(publication_id((1 << 63) - 1)) == len(b"urn:h2h:gallery:") + 19
    assert len(artifact_name((1 << 63) - 1)) == len(b"h2h-") + 19 + len(b".cbz")


@pytest.mark.parametrize(
    "gid_ascii",
    (
        b"",
        b"00",
        b"01",
        b"+1",
        b"-1",
        b" 1",
        b"1 ",
        b"\t1",
        b"1\n",
        b"1.0",
        b"1_0",
        b"\xff",
        "\N{FULLWIDTH DIGIT ONE}".encode(),
        "\N{ARABIC-INDIC DIGIT ONE}".encode(),
    ),
)
def test_publication_id_and_artifact_name_decoders_reject_noncanonical_gid_text(
    gid_ascii: bytes,
) -> None:
    with pytest.raises(ByteDomainError, match="gid|leading zero"):
        decode_publication_id(b"urn:h2h:gallery:" + gid_ascii)
    with pytest.raises(ByteDomainError, match="gid|leading zero"):
        decode_artifact_name(b"h2h-" + gid_ascii + b".cbz")


@pytest.mark.parametrize("gid_ascii", (b"0", b"9223372036854775808"))
def test_publication_id_and_artifact_name_decoders_reject_gid_outside_int63(
    gid_ascii: bytes,
) -> None:
    with pytest.raises(IntegerDomainError, match="publication_id gid"):
        decode_publication_id(b"urn:h2h:gallery:" + gid_ascii)
    with pytest.raises(IntegerDomainError, match="artifact_name gid"):
        decode_artifact_name(b"h2h-" + gid_ascii + b".cbz")


@pytest.mark.parametrize(
    "encoded",
    (
        b"",
        b"URN:h2h:gallery:1",
        b"urn:h2h:galleries:1",
        b"urn:h2h:gallery-id:1",
        b"xurn:h2h:gallery:1",
    ),
)
def test_publication_id_decoder_rejects_wrong_registered_prefix(
    encoded: bytes,
) -> None:
    with pytest.raises(ByteDomainError, match="prefix"):
        decode_publication_id(encoded)


@pytest.mark.parametrize(
    "encoded",
    (
        b"h2h-1",
        b"h2h-1.CBZ",
        b"h2h-1.cbz.extra",
        b"h2h-1.cbz\x00",
    ),
)
def test_artifact_name_decoder_rejects_wrong_registered_suffix(
    encoded: bytes,
) -> None:
    with pytest.raises(ByteDomainError, match="suffix"):
        decode_artifact_name(encoded)


@pytest.mark.parametrize(
    "encoded",
    (
        b"",
        b"H2H-1.cbz",
        b"artifact-1.cbz",
        b"xh2h-1.cbz",
    ),
)
def test_artifact_name_decoder_rejects_wrong_registered_prefix(
    encoded: bytes,
) -> None:
    with pytest.raises(ByteDomainError, match="prefix"):
        decode_artifact_name(encoded)


def test_publication_id_and_artifact_name_decoders_require_bounded_bytes() -> None:
    with pytest.raises(ByteDomainError, match="immutable bytes"):
        decode_publication_id(bytearray(b"urn:h2h:gallery:1"))  # type: ignore[arg-type]
    with pytest.raises(ByteDomainError, match="immutable bytes"):
        decode_artifact_name(memoryview(b"h2h-1.cbz"))  # type: ignore[arg-type]
    with pytest.raises(ByteDomainError, match="exceeds 64 bytes"):
        decode_publication_id(b"urn:h2h:gallery:" + b"1" * 49)
    with pytest.raises(ByteDomainError, match="exceeds 27 bytes"):
        decode_artifact_name(b"h2h-" + b"1" * 20 + b".cbz")


@pytest.mark.parametrize("value", ["", "bad/domain", "bad\x00domain", "é"])
def test_digest_domain_rejects_unregistered_identifier_shapes(value: str) -> None:
    with pytest.raises(RegisteredIdentifierError):
        canonical_value_digest(value, b"payload")


@pytest.mark.parametrize("value", [-1, 0, 1 << 63, (1 << 64) - 1, True])
def test_gid_rejects_non_positive_or_out_of_int63_values(value: int) -> None:
    with pytest.raises(IntegerDomainError):
        publication_id(value)
    with pytest.raises(IntegerDomainError):
        artifact_id(value, bytes(32))
    with pytest.raises(IntegerDomainError):
        publication_key(value)


@pytest.mark.parametrize("version", [-1, 0, 2, 1 << 32, True])
def test_gallery_key_rejects_invalid_or_unregistered_versions(version: int) -> None:
    with pytest.raises(IntegerDomainError):
        gallery_key(bytes(32), bytes(32), algorithm_version=version)
    with pytest.raises(IntegerDomainError):
        publication_key(1, algorithm_version=version)


def test_binary_digest_boundary_and_explicit_hex_helpers() -> None:
    digest = hashlib.sha256(b"payload").digest()

    assert digest_from_hex(digest_to_hex(digest)) == digest
    with pytest.raises(DigestFormatError):
        digest_to_hex(bytearray(digest))  # type: ignore[arg-type]
    with pytest.raises(DigestFormatError):
        digest_from_hex(digest.hex().upper())
    with pytest.raises(DigestFormatError):
        digest_from_hex(digest.hex()[:-1])
    with pytest.raises(DigestFormatError):
        artifact_id(1, b"short")


def test_canonical_identity_validation_and_conflict_guard() -> None:
    digest = canonical_value_digest("exact_payload_v1", b"exact payload")

    validate_canonical_value_identity(
        digest, digest_domain="exact_payload_v1", payload=b"exact payload"
    )
    verify_canonical_value_conflict(
        digest,
        "exact_payload_v1",
        b"exact payload",
        digest_domain="exact_payload_v1",
        payload=b"exact payload",
    )

    with pytest.raises(DigestMismatchError):
        validate_canonical_value_identity(
            bytes(32), digest_domain="exact_payload_v1", payload=b"exact payload"
        )
    with pytest.raises(DigestMismatchError):
        verify_canonical_value_conflict(
            bytes(32),
            "exact_payload_v1",
            b"exact payload",
            digest_domain="exact_payload_v1",
            payload=b"exact payload",
        )
    with pytest.raises(CanonicalIdentityCollisionError):
        verify_canonical_value_conflict(
            digest,
            "exact_payload_v1",
            b"different existing payload",
            digest_domain="exact_payload_v1",
            payload=b"exact payload",
        )
    with pytest.raises(CanonicalIdentityCollisionError):
        verify_canonical_value_conflict(
            digest,
            "another_payload_v1",
            b"exact payload",
            digest_domain="exact_payload_v1",
            payload=b"exact payload",
        )


def test_seeded_property_loop_matches_independent_encoders() -> None:
    generator = random.Random(0x4832484442)
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz 0123456789"

    for _ in range(256):
        digest_domain = f"test_domain_{generator.randrange(1, 1 << 32)}"
        payload = generator.randbytes(generator.randrange(0, 513))
        expected_digest = _manual_canonical_digest(digest_domain, payload)
        assert canonical_value_digest(digest_domain, payload) == expected_digest
        assert digest_from_hex(digest_to_hex(expected_digest)) == expected_digest

        scope_key = generator.randbytes(32)
        leaf = "".join(
            generator.choice(alphabet) for _ in range(generator.randrange(1, 80))
        )
        locator_sha256 = source_relative_locator_digest(
            "source_relative_locator_v1", ("nested", leaf)
        )
        expected_gallery_key = _manual_gallery_key(1, scope_key, locator_sha256)
        assert gallery_key(scope_key, locator_sha256) == expected_gallery_key
        assert gallery_key(scope_key, locator_sha256) == gallery_key(
            scope_key, locator_sha256
        )


def test_source_root_v1_independent_golden_vectors_and_streaming_parts() -> None:
    root_payload = bytes.fromhex("0000000100000000")
    nested_payload = bytes.fromhex(
        "000000010000000200000007566f6c756d657300000008e8b387e696992041"
    )

    assert encode_source_root(()) == root_payload
    assert b"".join(iter_source_root_payload(())) == root_payload
    assert source_root_digest(()) == bytes.fromhex(
        "25d5c20861a8646652543d9727df88fbef23e53d6ef050b04d1ae7199cbdf75a"
    )
    assert encode_source_root(("Volumes", "資料 A")) == nested_payload
    assert b"".join(iter_source_root_payload(("Volumes", "資料 A"))) == nested_payload
    assert decode_source_root(root_payload) == ()
    assert decode_source_root(nested_payload) == ("Volumes", "資料 A")
    root_receipt = validate_source_root_parts(
        (nested_payload[:3], nested_payload[3:17], nested_payload[17:])
    )
    assert isinstance(root_receipt, SourceRootValidationReceipt)
    assert root_receipt.component_count == 2
    assert root_receipt.payload_byte_count == len(nested_payload)
    assert root_receipt.payload_sha256 == hashlib.sha256(nested_payload).digest()
    assert source_root_digest(("Volumes", "資料 A")) == bytes.fromhex(
        "c5dfe7438afac5f4289a305456d424379a90a1a172b12dbc84973462b841a0f2"
    )


def test_source_root_v1_is_exact_and_rejects_non_posix_segments() -> None:
    assert decode_source_root(encode_source_root(("a\\b",))) == ("a\\b",)
    assert source_root_digest(("é",)) != source_root_digest(("e\u0301",))
    for segments in (("",), (".",), ("..",), ("a/b",), ("bad\x00name",)):
        with pytest.raises(ByteDomainError):
            encode_source_root(segments)
    with pytest.raises(ByteDomainError):
        encode_source_root(("bad\ud800name",))
    for malformed in (
        b"",
        bytes.fromhex("0000000200000000"),
        bytes.fromhex("000000010000000100000001ff"),
        encode_source_root(("a",)) + b"trailing",
    ):
        with pytest.raises((ByteDomainError, IntegerDomainError)):
            decode_source_root(malformed)
        with pytest.raises((ByteDomainError, IntegerDomainError)):
            validate_source_root_parts((malformed,))


def test_canonical_value_digest_parts_frames_declared_length_before_one_shot_data() -> (
    None
):
    payload = b"a" * 32767 + "資料".encode() + b"z"
    seen = 0

    def one_shot_parts() -> object:
        nonlocal seen
        seen += 1
        yield payload[:7]
        yield payload[7:32768]
        yield payload[32768:]

    assert canonical_value_digest_parts(
        "streamed_test_v1",
        len(payload),
        one_shot_parts(),  # type: ignore[arg-type]
    ) == _manual_canonical_digest("streamed_test_v1", payload)
    assert seen == 1
    with pytest.raises(ByteDomainError, match="exceed"):
        canonical_value_digest_parts("streamed_test_v1", 1, (b"ab",))
    with pytest.raises(ByteDomainError, match="do not equal"):
        canonical_value_digest_parts("streamed_test_v1", 2, (b"a",))
    with pytest.raises(IntegerDomainError):
        canonical_value_digest_parts("streamed_test_v1", 1 << 63, ())


def test_canonical_value_empty_page_has_independent_golden_and_exact_collision_guard() -> (
    None
):
    owner = bytes.fromhex("11" * 32)
    expected_page = bytes.fromhex(
        "68326864622d766e6578742d63616e6f6e6963616c2d76616c75652d7061676500"
        "00000001"
        + "11" * 32
        + "0000"
        + "0000000000000000"
        + "0000000000000000"
        + "00000000"
    )
    expected_sha = bytes.fromhex(
        "861916949e1425755a31697aa72ca809c5dd94dbfe59f8a184fb88743b13acf8"
    )

    tree = build_canonical_value_tree(owner, 0, ())

    assert tree.pages[0].page_bytes == expected_page
    assert tree.root_page_sha256 == expected_sha
    assert canonical_value_page_digest(expected_page) == expected_sha
    assert decode_canonical_value_page(expected_page) == CanonicalValuePage(
        owner,
        GalleryObservationNodeKind.LEAF,
        0,
        0,
        0,
        (),
    )
    validate_canonical_value_tree(tree)
    verify_canonical_value_page_conflict(expected_sha, expected_page, expected_page)
    with pytest.raises(CanonicalIdentityCollisionError):
        verify_canonical_value_page_conflict(
            expected_sha, expected_page, expected_page[:-1] + b"\x01"
        )
    with pytest.raises(ByteDomainError):
        decode_canonical_value_page(expected_page + b"\x00")


@pytest.mark.parametrize(
    "byte_count",
    [1, 32767, 32768, 32769, 65536, 65537],
)
def test_canonical_value_tree_chunk_boundaries_are_unique_and_minimal(
    byte_count: int,
) -> None:
    payload = bytes(index % 251 for index in range(byte_count))
    owner = canonical_value_digest("canonical_boundary_v1", payload)
    parts = (payload[:3], payload[3:32769], payload[32769:])

    tree = build_canonical_value_tree(owner, byte_count, parts)

    validate_canonical_value_tree(tree)
    assert tree.byte_count == byte_count
    leaf_pages = [
        decode_canonical_value_page(page.page_bytes)
        for page in tree.pages
        if decode_canonical_value_page(page.page_bytes).level == 0
    ]
    assert (
        len(leaf_pages)
        == (byte_count + CANONICAL_VALUE_CHUNK_BYTES - 1) // CANONICAL_VALUE_CHUNK_BYTES
    )
    assert [page.page_position for page in leaf_pages] == list(range(len(leaf_pages)))
    assert sum(page.subtree_byte_count for page in leaf_pages) == byte_count


def test_canonical_value_page_rejects_impossible_offset_and_owner_mix() -> None:
    owner = bytes.fromhex("22" * 32)
    with pytest.raises(ByteDomainError, match="page_position"):
        CanonicalValuePage(
            owner,
            GalleryObservationNodeKind.LEAF,
            0,
            1,
            1,
            # Offset zero cannot belong to page position one.
            (CanonicalValueChunk(0, b"x"),),
        )
    tree = build_canonical_value_tree(owner, 1, (b"x",))
    mixed = CanonicalValueTree(
        bytes.fromhex("33" * 32), tree.root_page_sha256, 1, tree.pages
    )
    with pytest.raises(ByteDomainError, match="cross-owner"):
        validate_canonical_value_tree(mixed)


def _observation_file_entries(count: int) -> tuple[GalleryObservationFileEntry, ...]:
    return tuple(
        GalleryObservationFileEntry(
            index,
            hashlib.sha256(b"key" + index.to_bytes(8, "big")).digest(),
            hashlib.sha256(b"file" + index.to_bytes(8, "big")).digest(),
            index,
            1 << 63 if index == 0 else index,
            (1 << 64) - 1 if index == 0 else index,
            -index,
            index,
        )
        for index in range(count)
    )


def _observation_tag_entries(count: int) -> tuple[GalleryObservationTagEntry, ...]:
    return tuple(
        GalleryObservationTagEntry(
            index,
            "藝術家" if index % 2 else "",
            hashlib.sha256(b"tag" + index.to_bytes(8, "big")).digest(),
        )
        for index in range(count)
    )


def _observation_directory_entries(
    count: int,
) -> tuple[GalleryObservationDirectoryEntry, ...]:
    names = [f"{index:08d}".encode() for index in range(count)]
    if count:
        names[0] = b"00000000\\opaque-\xff"
        names.sort()
    return tuple(
        GalleryObservationDirectoryEntry(
            index,
            name,
            index,
            1 << 63 if index == 0 else index,
            (1 << 64) - 1 if index == 0 else index,
            -index,
            index,
            GalleryObservationDirectoryFileType.REGULAR,
        )
        for index, name in enumerate(names)
    )


@pytest.mark.parametrize(
    ("component", "capacity", "factory"),
    [
        (
            GalleryObservationComponent.FILE,
            GALLERY_OBSERVATION_FILE_LEAF_CAPACITY,
            _observation_file_entries,
        ),
        (
            GalleryObservationComponent.TAG,
            GALLERY_OBSERVATION_TAG_LEAF_CAPACITY,
            _observation_tag_entries,
        ),
        (
            GalleryObservationComponent.DIRECTORY,
            GALLERY_OBSERVATION_DIRECTORY_LEAF_CAPACITY,
            _observation_directory_entries,
        ),
    ],
)
def test_observation_tree_empty_exact_and_double_capacity_boundaries(
    component: GalleryObservationComponent,
    capacity: int,
    factory: object,
) -> None:
    for count in (
        0,
        1,
        capacity - 1,
        capacity,
        capacity + 1,
        2 * capacity,
        2 * capacity + 1,
    ):
        entries = factory(count)  # type: ignore[operator]
        tree = build_gallery_observation_tree(component, entries)
        validate_gallery_observation_tree(tree)
        assert tree.item_count == count
        leaves = [
            decode_gallery_observation_page(page.page_bytes)
            for page in tree.pages
            if decode_gallery_observation_page(page.page_bytes).level == 0
        ]
        expected_leaf_count = 1 if count == 0 else (count + capacity - 1) // capacity
        assert len(leaves) == expected_leaf_count
        if count:
            assert all(len(page.entries) == capacity for page in leaves[:-1])
            assert 1 <= len(leaves[-1].entries) <= capacity
        else:
            assert leaves[0].entries == ()


def test_observation_empty_file_page_independent_golden_and_bounds() -> None:
    expected_page = bytes.fromhex(
        "68326864622d766e6578742d67616c6c6572792d6f62736572766174696f6e2d7061676500"
        "00000001000000"
        "0000000000000000"
        "00000000"
    )
    expected_sha = bytes.fromhex(
        "ae1f20c15fc8e7738156d983fde8c8d023a346cdabc77776fac55c0bc8bb6150"
    )
    tree = build_gallery_observation_tree(GalleryObservationComponent.FILE, ())
    page = decode_gallery_observation_page(expected_page)

    assert tree.pages[0].page_bytes == expected_page
    assert tree.root_page_sha256 == expected_sha
    assert gallery_observation_page_digest(expected_page) == expected_sha
    assert gallery_observation_page_key_bounds(page) is None
    verify_gallery_observation_page_conflict(expected_sha, expected_page, expected_page)
    with pytest.raises(CanonicalIdentityCollisionError):
        verify_gallery_observation_page_conflict(
            expected_sha, expected_page, expected_page[:-1] + b"\x01"
        )
    with pytest.raises(ByteDomainError):
        gallery_observation_page_digest(b"")
    with pytest.raises(ByteDomainError):
        gallery_observation_page_digest(b"not-a-page")


def test_observation_directory_maximum_record_page_and_numeric_raw_boundaries() -> None:
    entries = tuple(
        GalleryObservationDirectoryEntry(
            index,
            f"{index:03d}".encode() + b"x" * 252,
            (1 << 63) - 1,
            1 << 63,
            (1 << 64) - 1,
            -(1 << 63),
            (1 << 63) - 1,
            GalleryObservationDirectoryFileType.OTHER,
        )
        for index in range(GALLERY_OBSERVATION_DIRECTORY_LEAF_CAPACITY)
    )
    tree = build_gallery_observation_tree(
        GalleryObservationComponent.DIRECTORY, entries
    )
    leaf = tree.pages[0]

    assert len(leaf.page_bytes) == 59768
    decoded = decode_gallery_observation_page(leaf.page_bytes)
    assert decoded.entries[0] == entries[0]
    assert gallery_observation_page_key_bounds(decoded) == (
        entries[0].name_bytes,
        entries[-1].name_bytes,
    )


def test_observation_file_and_tag_ordinals_are_globally_zero_based_contiguous() -> None:
    with pytest.raises(ByteDomainError, match="zero-based canonical"):
        build_gallery_observation_tree(
            GalleryObservationComponent.FILE,
            (_observation_file_entries(1)[0], _observation_file_entries(3)[2]),
        )
    with pytest.raises(ByteDomainError, match="zero-based canonical"):
        build_gallery_observation_tree(
            GalleryObservationComponent.TAG,
            (_observation_tag_entries(1)[0], _observation_tag_entries(3)[2]),
        )


def _metadata_fixture(
    *, title: str = "a", comment: str = "藝術家"
) -> GalleryObservationMetadata:
    return GalleryObservationMetadata(
        7,
        title,
        comment,
        "acct",
        11,
        12,
        13,
        1,
        2,
        3,
    )


def test_observation_metadata_exact_golden_roundtrip_and_every_split_checkpoint() -> (
    None
):
    metadata = _metadata_fixture()
    payload = encode_gallery_observation_metadata(metadata)
    expected = bytes.fromhex(
        "68326864622d766e6578742d67616c6c6572792d6f62736572766174696f6e2d6d6574616461746100"
        "00000001"
        "0000000000000007"
        "01"
        "0000000000000001"
        "61"
        "02"
        "0000000000000009"
        "e8979de8a193e5aeb6"
        "03"
        "0000000000000004"
        "61636374"
        "000000000000000b"
        "000000000000000c"
        "000000000000000d"
        "00000001"
        "0000000000000002"
        "01"
        "00000003"
    )

    assert payload == expected
    assert decode_gallery_observation_metadata(payload) == metadata
    for split in range(len(payload) + 1):
        receipt = validate_gallery_observation_metadata_parts(
            (payload[:split], payload[split:])
        )
        assert receipt.gid == metadata.gid
        assert receipt.comment_byte_count == len(metadata.comment.encode())

    decoder = GalleryObservationMetadataDecoder()
    for byte in payload:
        decoder.feed(bytes((byte,)))
        decoder = GalleryObservationMetadataDecoder(decoder.state)
    assert decoder.finish().page_count == 3


def test_observation_metadata_stream_crosses_page_and_utf8_boundaries() -> None:
    metadata = _metadata_fixture(
        title="x" * (GALLERY_OBSERVATION_METADATA_CHUNK_BYTES - 8)
        + "資料"
        + "y" * 40000,
        comment="é" * 20000,
    )
    payload = encode_gallery_observation_metadata(metadata)
    tree = build_gallery_observation_metadata_tree(metadata)
    parts = tuple(iter_gallery_observation_metadata_stream(metadata))

    assert len(payload) > 65536
    assert b"".join(parts) == payload
    validate_gallery_observation_tree(tree)
    page_chunks = []
    for encoded in tree.pages:
        page = decode_gallery_observation_page(encoded.page_bytes)
        if page.level == 0:
            (chunk,) = page.entries
            assert type(chunk) is GalleryObservationMetadataChunk
            page_chunks.append(chunk.chunk_bytes)
    receipt = validate_gallery_observation_metadata_parts(page_chunks)
    assert receipt.title_byte_count == len(metadata.title.encode())
    assert receipt.comment_byte_count == len(metadata.comment.encode())


def test_observation_metadata_incremental_parser_rejects_forged_and_malformed_state() -> (
    None
):
    with pytest.raises(IntegerDomainError):
        GalleryObservationMetadataDecoderState(
            "DONE", b"", 0, b"", 0, (0, 0, 0), -1, -2, -3, 0, -4, 1 << 40
        )
    with pytest.raises(ByteDomainError, match="text carry"):
        GalleryObservationMetadataDecoder(
            GalleryObservationMetadataDecoderState(
                "DONE",
                b"",
                1,
                b"",
                1,
                (0, 0, 0),
                0,
                0,
                0,
                1,
                0,
                None,
            )
        )
    with pytest.raises(ByteDomainError, match="exceeds its declared"):
        GalleryObservationMetadataDecoder(
            GalleryObservationMetadataDecoderState(
                "TITLE_TEXT",
                b"",
                2,
                b"",
                1,
                (1, 0, 0),
                None,
                None,
                None,
                None,
                None,
                None,
            )
        )
    with pytest.raises(ByteDomainError, match="future metadata text lengths"):
        GalleryObservationMetadataDecoder(
            GalleryObservationMetadataDecoderState(
                "TITLE_LENGTH",
                b"",
                0,
                b"",
                1,
                (0, 1, 0),
                None,
                None,
                None,
                None,
                None,
                None,
            )
        )
    with pytest.raises(ByteDomainError, match="incomplete code point"):
        GalleryObservationMetadataDecoder(
            GalleryObservationMetadataDecoderState(
                "TITLE_TEXT",
                b"",
                1,
                b"a",
                1,
                (2, 0, 0),
                None,
                None,
                None,
                None,
                None,
                None,
            )
        )

    payload = bytearray(encode_gallery_observation_metadata(_metadata_fixture()))
    title_marker = payload.index(b"\x01" + (1).to_bytes(8, "big"))
    malformed_values = [
        bytes(payload[:-1]),
        bytes(payload) + b"\x00",
        bytes(b"X" + payload[1:]),
        bytes(payload[:title_marker] + b"\x09" + payload[title_marker + 1 :]),
        bytes(payload[: title_marker + 9] + b"\xff" + payload[title_marker + 10 :]),
    ]
    for malformed in malformed_values:
        with pytest.raises((ByteDomainError, IntegerDomainError)):
            validate_gallery_observation_metadata_parts(
                malformed[index : index + 1] for index in range(len(malformed))
            )


def test_observation_metadata_page_is_structural_until_full_tree_semantic_validation() -> (
    None
):
    with pytest.raises(ByteDomainError, match="exactly one chunk"):
        GalleryObservationPage(
            GalleryObservationComponent.METADATA,
            GalleryObservationNodeKind.LEAF,
            0,
            0,
            (),
        )
    page = GalleryObservationPage(
        GalleryObservationComponent.METADATA,
        GalleryObservationNodeKind.LEAF,
        0,
        1,
        (GalleryObservationMetadataChunk(0, b"x"),),
    )
    page_bytes = encode_gallery_observation_page(page)
    digest = gallery_observation_page_digest(page_bytes)
    tree = GalleryObservationTree(
        GalleryObservationComponent.METADATA,
        digest,
        1,
        (GalleryObservationEncodedPage(digest, page_bytes),),
    )
    with pytest.raises(ByteDomainError, match="metadata"):
        validate_gallery_observation_tree(tree)


def test_observation_descriptor_is_fixed_bounded_and_domain_separated() -> None:
    descriptor = GalleryObservationDescriptor(
        bytes.fromhex("01" * 32),
        123,
        bytes.fromhex("02" * 32),
        2,
        bytes.fromhex("03" * 32),
        3,
        bytes.fromhex("04" * 32),
        4,
    )
    payload = encode_gallery_observation_descriptor(descriptor)

    assert len(payload) == 164
    assert decode_gallery_observation_descriptor(payload) == descriptor
    assert gallery_observation_descriptor_digest(
        descriptor
    ) == _manual_canonical_digest("gallery_observation_v1", payload)
    with pytest.raises(ByteDomainError):
        decode_gallery_observation_descriptor(payload + b"\x00")


def test_observation_audit_frames_and_durable_parser_registry_are_closed() -> None:
    root = bytes(range(32))
    assert gallery_directory_audit_digest(root, 7).hex() == (
        "75cc50ba337da95a237aaecf984016af9ea667a80492aba5b0cadda10a16b9d7"
    )
    assert gallery_metadata_audit_digest(root, 11).hex() == (
        "70bdb1d71ac3f6daabf19ac16334acd825149f65054bbe3384602b7e8a1d6d06"
    )
    roots = {
        component: (bytes((int(component) + 1,)) * 32, int(component) + 3)
        for component in GalleryObservationComponent
    }
    assert gallery_scan_audit_digest(roots).hex() == (
        "b04306743e234c19165a72e2e4bc7831b6a90bb397f6c7a69fdee6fb03ab7042"
    )
    mutated = dict(roots)
    mutated[GalleryObservationComponent.FILE] = (b"z" * 32, 3)
    assert gallery_scan_audit_digest(mutated) != gallery_scan_audit_digest(roots)
    with pytest.raises(ByteDomainError, match="exact component set"):
        gallery_scan_audit_digest(
            {
                component: value
                for component, value in roots.items()
                if component is not GalleryObservationComponent.DIRECTORY
            }
        )
    with pytest.raises(ByteDomainError, match="exact component set"):
        gallery_scan_audit_digest({**roots, 99: (b"x" * 32, 1)})  # type: ignore[dict-item]
    with pytest.raises(DigestFormatError):
        gallery_directory_audit_digest(root[:-1], 7)
    with pytest.raises(IntegerDomainError):
        gallery_metadata_audit_digest(root, -1)

    assert GALLERY_OBSERVATION_DURABLE_PARSER_PHASES == (
        "PREFIX",
        "VERSION",
        "GID",
        "TITLE_TAG",
        "TITLE_LENGTH",
        "TITLE",
        "COMMENT_TAG",
        "COMMENT_LENGTH",
        "COMMENT",
        "UPLOAD_ACCOUNT_TAG",
        "UPLOAD_ACCOUNT_LENGTH",
        "UPLOAD_ACCOUNT",
        "UPLOAD_TIME",
        "DOWNLOAD_TIME",
        "MODIFIED_TIME",
        "SCAN_VERSION",
        "SOURCE_FILE_COUNT",
        "PAGE_COUNT_PRESENCE",
        "PAGE_COUNT",
        "DONE",
    )
    for phase in GALLERY_OBSERVATION_DURABLE_PARSER_PHASES:
        assert validate_gallery_observation_durable_parser_phase(phase) == phase.encode(
            "ascii"
        )
    for alias in (
        "TITLE_TEXT",
        "COMMENT_TEXT",
        "ACCOUNT_TAG",
        "ACCOUNT_LENGTH",
        "ACCOUNT_TEXT",
        "LENGTH",
        "unknown",
    ):
        with pytest.raises(RegisteredIdentifierError, match="not a registered"):
            validate_gallery_observation_durable_parser_phase(alias)
