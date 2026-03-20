{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg_ar_pymnt_term', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CG_AR_PYMNT_TERM',
        'target_table': 'ST_CG_AR_PYMNT_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.491648+00:00'
    }
) }}

WITH 

source_cg1_ra_terms_tl AS (
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
        term_id
    FROM {{ source('raw', 'cg1_ra_terms_tl') }}
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
    FROM source_cg1_ra_terms_tl
)

SELECT * FROM final