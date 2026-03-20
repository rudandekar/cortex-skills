{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_purchasing_item_category', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PURCHASING_ITEM_CATEGORY',
        'target_table': 'N_PURCHASING_ITEM_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.110640+00:00'
    }
) }}

WITH 

source_w_purchasing_item_category AS (
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
    FROM {{ source('raw', 'w_purchasing_item_category') }}
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
        edw_update_dtm
    FROM source_w_purchasing_item_category
)

SELECT * FROM final