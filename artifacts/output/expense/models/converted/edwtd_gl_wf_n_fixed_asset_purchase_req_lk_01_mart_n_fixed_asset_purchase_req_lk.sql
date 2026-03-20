{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fixed_asset_purchase_req_lk', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIXED_ASSET_PURCHASE_REQ_LK',
        'target_table': 'N_FIXED_ASSET_PURCHASE_REQ_LK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.628844+00:00'
    }
) }}

WITH 

source_w_fixed_asset_purchase_req_lk AS (
    SELECT
        bk_fixed_asset_num,
        bk_company_cd,
        set_of_books_key,
        bk_purchase_rqstn_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fixed_asset_purchase_req_lk') }}
),

final AS (
    SELECT
        bk_fixed_asset_num,
        bk_company_cd,
        set_of_books_key,
        bk_purchase_rqstn_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_fixed_asset_purchase_req_lk
)

SELECT * FROM final