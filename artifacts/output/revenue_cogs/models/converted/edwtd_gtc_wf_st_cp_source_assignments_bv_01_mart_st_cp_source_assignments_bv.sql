{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cp_source_assignments_bv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CP_SOURCE_ASSIGNMENTS_BV',
        'target_table': 'ST_CP_SOURCE_ASSIGNMENTS_BV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.935385+00:00'
    }
) }}

WITH 

source_bv_cp_source_assignments AS (
    SELECT
        source_assignment_id,
        source_assignment_name,
        source_group_id,
        source_id,
        instance_id,
        set_of_books_id,
        set_of_books_name,
        application_usage_id,
        org_id,
        org_name,
        transaction_table,
        disable_source,
        copy_source,
        active,
        stamp_parent_trx,
        external_table,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        sae_create_row_flag,
        sae_trace_flag,
        period_set_name_id,
        sae_upate_existing_flag,
        trace_flag,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'bv_cp_source_assignments') }}
),

final AS (
    SELECT
        source_assignment_id,
        source_assignment_name,
        source_group_id,
        source_id,
        instance_id,
        set_of_books_id,
        set_of_books_name,
        application_usage_id,
        org_id,
        org_name,
        transaction_table,
        disable_source,
        copy_source,
        active,
        stamp_parent_trx,
        external_table,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        sae_create_row_flag,
        sae_trace_flag,
        period_set_name_id,
        sae_upate_existing_flag,
        trace_flag,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM source_bv_cp_source_assignments
)

SELECT * FROM final