{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_mtl_categories_b_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_MTL_CATEGORIES_B_TL',
        'target_table': 'ST_MF_MTL_CATEGORIES_B_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.719133+00:00'
    }
) }}

WITH 

source_ff_mf_mtl_categories_b_tl AS (
    SELECT
        batch_id,
        category_id,
        global_name,
        description,
        language_code,
        ges_update_date,
        segment1,
        attribute2,
        attribute3,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_mtl_categories_b_tl') }}
),

final AS (
    SELECT
        batch_id,
        category_id,
        global_name,
        description,
        language_code,
        ges_update_date,
        segment1,
        attribute2,
        attribute3,
        create_datetime,
        action_code
    FROM source_ff_mf_mtl_categories_b_tl
)

SELECT * FROM final