
NOT EXISTS (SELECT 1 FROM operational_canonical_value_uploads x
            WHERE x.value_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM operational_hash_cache_observations x
                WHERE x.source_identity_sha256 = r.value_sha256
                   OR x.fingerprint_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_scopes scope_root
    JOIN catalog_source_build_descriptor build
      ON build.scope_key = scope_root.scope_key
    WHERE scope_root.source_root_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_scopes scope_root
    JOIN catalog_gallery_identities gallery
      ON gallery.scope_key = scope_root.scope_key
    WHERE scope_root.source_root_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_gallery_identities x
                WHERE x.locator_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observations x
                WHERE x.observation_identity_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_gallery_observation_tags x
    JOIN catalog_tag_terms term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_gallery_observation_artists x
    JOIN catalog_tag_terms term ON term.tag_id = x.artist_tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_subjects x
    JOIN catalog_tag_terms term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_head_revisions current_source
    JOIN catalog_source_revision_descriptors manifest
      ON manifest.source_revision = current_source.source_revision
    WHERE manifest.snapshot_manifest_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM operational_source_working_builds working
    JOIN catalog_analysis_run_descriptor live_analysis
      ON live_analysis.build_id = working.build_id
    JOIN catalog_analysis_snapshot_manifest manifest
      ON manifest.analysis_id = live_analysis.analysis_id
    WHERE manifest.snapshot_manifest_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_candidates live_analysis
    JOIN catalog_analysis_snapshot_manifest manifest
      ON manifest.analysis_id = live_analysis.analysis_id
    WHERE manifest.snapshot_manifest_sha256 = r.value_sha256
      AND (
        EXISTS (
            SELECT 1 FROM operational_catalog_working_candidates working
            WHERE working.candidate_id = live_analysis.candidate_id)
        OR NOT EXISTS (
            SELECT 1 FROM catalog_publication_commits committed
            WHERE committed.candidate_id = live_analysis.candidate_id)))
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_provenance x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_impacted_content x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_candidate_shadows x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_shadows x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_tombstones x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_candidates x
    JOIN catalog_artifact_policies policy
      ON policy.artifact_policy_id = x.artifact_policy_id
    WHERE policy.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    JOIN catalog_artifact_policies policy
      ON policy.artifact_policy_id = committed.artifact_policy_id
    WHERE policy.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.source_manifest_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.member_plan_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.effective_content_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.selected_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.owner_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_candidate_artifact_inputs x
                WHERE x.artifact_semantics_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifacts x
                WHERE x.artifact_semantics_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_search_postings x
                WHERE x.value_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_language_facet_order x
                WHERE x.language_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_contributor_facet_order x
                WHERE x.contributor_name_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_tag_directory_order x
                WHERE x.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_tag_publication_order x
    JOIN catalog_tag_terms term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_subject_facet_order x
    JOIN catalog_tag_terms term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_contents x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_contributors x
                WHERE x.contributor_name_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.summary_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.language_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.source_title_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_display_title_choices choice
    WHERE (choice.source_title_sha256 = r.value_sha256
           OR choice.title_sha256 = r.value_sha256)
      AND (
    EXISTS (
        SELECT 1
        FROM catalog_publication_commit_head_receipts head
        JOIN catalog_publication_commits current_revision
          ON current_revision.receipt_id = head.receipt_id
        JOIN catalog_publication_titles current_title
          ON current_title.revision = current_revision.revision
        WHERE current_revision.display_title_policy_id = choice.display_title_policy_id
          AND current_title.source_title_sha256 = choice.source_title_sha256
          AND current_title.source_gallery_name = choice.source_gallery_name)
    OR EXISTS (
        SELECT 1
        FROM catalog_publication_candidates candidate_revision
        JOIN catalog_publication_titles candidate_title
          ON candidate_title.revision = candidate_revision.reserved_revision
        WHERE candidate_revision.display_title_policy_id = choice.display_title_policy_id
          AND candidate_title.source_title_sha256 = choice.source_title_sha256
          AND candidate_title.source_gallery_name = choice.source_gallery_name
          AND (
            EXISTS (
                SELECT 1 FROM operational_catalog_working_candidates working
                WHERE working.candidate_id = candidate_revision.candidate_id)
            OR NOT EXISTS (
                SELECT 1 FROM catalog_publication_commits committed
                WHERE committed.candidate_id = candidate_revision.candidate_id)))
    ))
AND NOT EXISTS (
    SELECT 1 FROM catalog_title_sorts title_sort
    WHERE (title_sort.title_sha256 = r.value_sha256
           OR title_sort.sort_title_sha256 = r.value_sha256)
      AND (
    EXISTS (
        SELECT 1
        FROM catalog_display_title_choices choice
        JOIN catalog_display_title_policies policy
          ON policy.display_title_policy_id = choice.display_title_policy_id
        WHERE policy.title_sort_policy_id = title_sort.title_sort_policy_id
          AND choice.title_sha256 = title_sort.title_sha256
          AND (
    EXISTS (
        SELECT 1
        FROM catalog_publication_commit_head_receipts head
        JOIN catalog_publication_commits current_revision
          ON current_revision.receipt_id = head.receipt_id
        JOIN catalog_publication_titles current_title
          ON current_title.revision = current_revision.revision
        WHERE current_revision.display_title_policy_id = choice.display_title_policy_id
          AND current_title.source_title_sha256 = choice.source_title_sha256
          AND current_title.source_gallery_name = choice.source_gallery_name)
    OR EXISTS (
        SELECT 1
        FROM catalog_publication_candidates candidate_revision
        JOIN catalog_publication_titles candidate_title
          ON candidate_title.revision = candidate_revision.reserved_revision
        WHERE candidate_revision.display_title_policy_id = choice.display_title_policy_id
          AND candidate_title.source_title_sha256 = choice.source_title_sha256
          AND candidate_title.source_gallery_name = choice.source_gallery_name
          AND (
            EXISTS (
                SELECT 1 FROM operational_catalog_working_candidates working
                WHERE working.candidate_id = candidate_revision.candidate_id)
            OR NOT EXISTS (
                SELECT 1 FROM catalog_publication_commits committed
                WHERE committed.candidate_id = candidate_revision.candidate_id)))
    ))
    ))
