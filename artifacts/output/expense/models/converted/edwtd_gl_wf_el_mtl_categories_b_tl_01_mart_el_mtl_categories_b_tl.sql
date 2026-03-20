{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_mtl_categories_b_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_MTL_CATEGORIES_B_TL',
        'target_table': 'EL_MTL_CATEGORIES_B_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.602771+00:00'
    }
) }}

WITH 

source_st_mf_mtl_categories_b_tl AS (
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
    FROM {{ source('raw', 'st_mf_mtl_categories_b_tl') }}
),

final AS (
    SELECT
        category_id,
        global_name,
        description,
        language_code,
        ges_update_date,
        segment1
    FROM source_st_mf_mtl_categories_b_tl
)

SELECT * FROM final