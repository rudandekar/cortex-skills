{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cdc_xxicm_dispute_headers_all', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CDC_XXICM_DISPUTE_HEADERS_ALL',
        'target_table': 'STG_CDC_XXICM_DISPUTE_HDRS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.845195+00:00'
    }
) }}

WITH 

source_cdc_xxicm_dispute_headers_all AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        saf_id,
        dispute_source,
        dispute_private,
        dispute_reason,
        dispute_date,
        dispute_status,
        dispute_total,
        dispute_caused_by,
        user_default_role_id,
        user_group_id,
        collector_id,
        comments,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login
    FROM {{ source('raw', 'cdc_xxicm_dispute_headers_all') }}
),

source_stg_cdc_xxicm_dispute_hdrs_all AS (
    SELECT
        saf_id,
        dispute_source,
        dispute_private,
        dispute_reason,
        dispute_date,
        dispute_status,
        dispute_total,
        dispute_caused_by,
        user_default_role_id,
        user_group_id,
        collector_id,
        comments,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cdc_xxicm_dispute_hdrs_all') }}
),

transformed_exp_cdc_xxicm_dispute_headers_all AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    saf_id,
    dispute_source,
    dispute_private,
    dispute_reason,
    dispute_date,
    dispute_status,
    dispute_total,
    dispute_caused_by,
    user_default_role_id,
    user_group_id,
    collector_id,
    comments,
    org_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login
    FROM source_stg_cdc_xxicm_dispute_hdrs_all
),

final AS (
    SELECT
        saf_id,
        dispute_source,
        dispute_private,
        dispute_reason,
        dispute_date,
        dispute_status,
        dispute_total,
        dispute_caused_by,
        user_default_role_id,
        user_group_id,
        collector_id,
        comments,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cdc_xxicm_dispute_headers_all
)

SELECT * FROM final