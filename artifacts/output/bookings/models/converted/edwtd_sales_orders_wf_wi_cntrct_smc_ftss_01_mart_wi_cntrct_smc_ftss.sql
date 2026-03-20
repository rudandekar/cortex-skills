{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cntrct_smc_ftss', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_CNTRCT_SMC_FTSS',
        'target_table': 'WI_CNTRCT_SMC_FTSS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.702955+00:00'
    }
) }}

WITH 

source_wi_cntrct_smc_ftss AS (
    SELECT
        svc_cntrct_ln_tech_svcs_key,
        ip_key,
        service_product_key,
        terminated_dtm,
        rnwd_svc_cntrct_ln_tch_svc_key,
        source_create_dtm,
        service_status_cd,
        sk_id_lint,
        cx_customer_bu_id,
        cl_inventory_item_id,
        cl_service_level,
        svc_cntrct_line_end_dtm,
        bk_svc_cntrct_line_start_dtm,
        maintenance_order_num,
        covered_product_id,
        dv_line_style_id_int
    FROM {{ source('raw', 'wi_cntrct_smc_ftss') }}
),

final AS (
    SELECT
        svc_cntrct_ln_tech_svcs_key,
        ip_key,
        service_product_key,
        terminated_dtm,
        rnwd_svc_cntrct_ln_tch_svc_key,
        source_create_dtm,
        service_status_cd,
        sk_id_lint,
        cx_customer_bu_id,
        cl_inventory_item_id,
        cl_service_level,
        svc_cntrct_line_end_dtm,
        bk_svc_cntrct_line_start_dtm,
        maintenance_order_num,
        covered_product_id,
        dv_line_style_id_int
    FROM source_wi_cntrct_smc_ftss
)

SELECT * FROM final