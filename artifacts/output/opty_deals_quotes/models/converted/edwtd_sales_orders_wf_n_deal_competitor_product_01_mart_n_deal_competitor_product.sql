{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_competitor_product', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_COMPETITOR_PRODUCT',
        'target_table': 'N_DEAL_COMPETITOR_PRODUCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.877608+00:00'
    }
) }}

WITH 

source_w_deal_competitor_product AS (
    SELECT
        bk_deal_id,
        bk_src_rptd_competitor_name,
        bk_competing_prdt_desc,
        competitor_prdt_type_cd,
        additional_info_txt,
        competing_technology_desc,
        src_last_update_dtm,
        dv_source_last_update_dt,
        data_source_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_competitor_product') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_src_rptd_competitor_name,
        bk_competing_prdt_desc,
        competitor_prdt_type_cd,
        additional_info_txt,
        competing_technology_desc,
        src_last_update_dtm,
        dv_source_last_update_dt,
        data_source_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_deal_competitor_product
)

SELECT * FROM final