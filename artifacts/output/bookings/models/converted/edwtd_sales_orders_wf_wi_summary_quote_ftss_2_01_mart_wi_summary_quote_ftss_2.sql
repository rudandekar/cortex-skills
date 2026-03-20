{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_summary_quote_ftss_2', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SUMMARY_QUOTE_FTSS_2',
        'target_table': 'WI_SUMMARY_QUOTE_FTSS_2',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.463898+00:00'
    }
) }}

WITH 

source_wi_summary_quote_ftss_2 AS (
    SELECT
        ib_svc_lvl,
        svc_cntrct_ln_tech_svcs_key,
        ip_key,
        bk_service_contract_num,
        service_status_cd,
        maintenance_order_num,
        maintenance_list_local_amt,
        maintenance_net_local_amt,
        sk_id_lint,
        service_product_key,
        source_create_dtm,
        source_application_name,
        case_detail_txt,
        summary_quote_case_num,
        sales_motion_cd,
        bk_svc_cntrct_line_start_dtm,
        svc_cntrct_line_end_dtm,
        terminated_dtm,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        cx_customer_bu_id,
        cl_inventory_item_id,
        covered_product_id,
        upgrade_product_number,
        dv_line_style_id_int,
        reason_code
    FROM {{ source('raw', 'wi_summary_quote_ftss_2') }}
),

final AS (
    SELECT
        ib_svc_lvl,
        svc_cntrct_ln_tech_svcs_key,
        ip_key,
        bk_service_contract_num,
        service_status_cd,
        maintenance_order_num,
        maintenance_list_local_amt,
        maintenance_net_local_amt,
        sk_id_lint,
        service_product_key,
        source_create_dtm,
        source_application_name,
        case_detail_txt,
        summary_quote_case_num,
        sales_motion_cd,
        bk_svc_cntrct_line_start_dtm,
        svc_cntrct_line_end_dtm,
        terminated_dtm,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        cx_customer_bu_id,
        cl_inventory_item_id,
        covered_product_id,
        upgrade_product_number,
        dv_line_style_id_int,
        reason_code
    FROM source_wi_summary_quote_ftss_2
)

SELECT * FROM final