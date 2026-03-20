{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cdc_ar_pymnt_term', 'realtime', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CDC_AR_PYMNT_TERM',
        'target_table': 'ST_CDC_AR_PYMNT_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.939794+00:00'
    }
) }}

WITH 

source_cdc_ra_terms_tl AS (
    SELECT
        created_by,
        creation_date,
        description,
        cg_language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        source_lang,
        term_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cdc_ra_terms_tl') }}
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        description,
        cg_language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        source_lang,
        term_id,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM source_cdc_ra_terms_tl
)

SELECT * FROM final