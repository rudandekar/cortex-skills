{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_purchasing_item_category', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_PURCHASING_ITEM_CATEGORY',
        'target_table': 'W_PURCHASING_ITEM_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.921428+00:00'
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

transformed_exp_wk_purchasing_item_category AS (
    SELECT
    bk_purchasing_item_category_id,
    item_category_name,
    attribute2,
    attribute3
    FROM source_st_mf_mtl_categories_b_tl
),

final AS (
    SELECT
        bk_purchasing_item_category_id,
        item_category_name,
        gps_spend_category_cd,
        gps_spend_sub_category_cd,
        edw_update_user,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM transformed_exp_wk_purchasing_item_category
)

SELECT * FROM final