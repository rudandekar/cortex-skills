{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cp_target_assignments_bv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CP_TARGET_ASSIGNMENTS_BV',
        'target_table': 'ST_CP_TARGET_ASSIGNMENTS_BV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.551687+00:00'
    }
) }}

WITH 

source_bv_cp_target_assignments AS (
    SELECT
        target_assignment_id,
        reg_id,
        target_type_id,
        acc_segment_value,
        target_set_of_books_id,
        set_of_books_name,
        target_org_id,
        org_name,
        target_instance_id,
        target_coa_id,
        balancing_segment,
        target_acct_currency,
        active,
        zero_flag,
        master,
        target_set_of_books,
        target_org,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        source_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'bv_cp_target_assignments') }}
),

final AS (
    SELECT
        target_assignment_id,
        reg_id,
        target_type_id,
        acc_segment_value,
        target_set_of_books_id,
        set_of_books_name,
        target_org_id,
        org_name,
        target_instance_id,
        target_coa_id,
        balancing_segment,
        target_acct_currency,
        active,
        zero_flag,
        master,
        target_set_of_books,
        target_org,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        source_id,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_bv_cp_target_assignments
)

SELECT * FROM final