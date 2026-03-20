{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_related_profile_hist', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_RELATED_PROFILE_HIST',
        'target_table': 'FF_CCRM_RELATED_PROFILE_HIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.976017+00:00'
    }
) }}

WITH 

source_ccrm_related_profile_hist AS (
    SELECT
        history_id,
        child_profile_id,
        profile_id,
        link_flag,
        fiscal_period,
        last_updated_by,
        last_update_date,
        linkage_type,
        link_type,
        processed,
        comments
    FROM {{ source('raw', 'ccrm_related_profile_hist') }}
),

transformed_exp_ccrm_related_profile_hist AS (
    SELECT
    history_id,
    child_profile_id,
    profile_id,
    link_flag,
    fiscal_period,
    last_updated_by,
    last_update_date,
    linkage_type,
    link_type,
    processed,
    comments,
    'BatchId' AS batchid_out,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_ccrm_related_profile_hist
),

final AS (
    SELECT
        batch_id,
        history_id,
        child_profile_id,
        profile_id,
        link_flag,
        fiscal_period,
        last_updated_by,
        last_update_date,
        linkage_type,
        link_type,
        processed,
        comments,
        create_datetime,
        action_code
    FROM transformed_exp_ccrm_related_profile_hist
)

SELECT * FROM final