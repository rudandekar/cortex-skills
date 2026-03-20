{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_fa_additions_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_FA_ADDITIONS_TL',
        'target_table': 'FF_MF_FA_ADDITIONS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.018985+00:00'
    }
) }}

WITH 

source_mf_fa_additions_tl AS (
    SELECT
        asset_id,
        created_by,
        creation_date,
        description,
        ges_update_date,
        global_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_lang
    FROM {{ source('raw', 'mf_fa_additions_tl') }}
),

transformed_exp_mf_fa_additions_tl AS (
    SELECT
    asset_id,
    created_by,
    creation_date,
    description,
    ges_update_date,
    global_name,
    language,
    last_updated_by,
    last_update_date,
    last_update_login,
    source_lang,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_fa_additions_tl
),

final AS (
    SELECT
        batch_id,
        asset_id,
        created_by,
        creation_date,
        description,
        ges_update_date,
        global_name,
        language_code,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_lang,
        create_datetime,
        action_code
    FROM transformed_exp_mf_fa_additions_tl
)

SELECT * FROM final