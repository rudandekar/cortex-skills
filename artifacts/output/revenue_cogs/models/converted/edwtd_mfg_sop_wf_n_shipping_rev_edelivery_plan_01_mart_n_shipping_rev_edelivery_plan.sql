{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_shipping_rev_edelivery_plan', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_N_SHIPPING_REV_EDELIVERY_PLAN',
        'target_table': 'N_SHIPPING_REV_EDELIVERY_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.213425+00:00'
    }
) }}

WITH 

source_w_shipping_rev_edelivery_plan AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_start_dt,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        bk_src_publish_dtm,
        edelivery_plan_rev_usd_amt,
        dv_src_publish_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_shipping_rev_edelivery_plan') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_start_dt,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        bk_src_publish_dtm,
        edelivery_plan_rev_usd_amt,
        dv_src_publish_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_shipping_rev_edelivery_plan
)

SELECT * FROM final