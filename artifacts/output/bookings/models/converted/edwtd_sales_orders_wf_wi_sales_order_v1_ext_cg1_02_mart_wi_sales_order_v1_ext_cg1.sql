{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_order_v1_ext_cg1', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_ORDER_V1_EXT_CG1',
        'target_table': 'WI_SALES_ORDER_V1_EXT_CG1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.815416+00:00'
    }
) }}

WITH 

source_wi_sales_order_v1_ext_cg1 AS (
    SELECT
        sales_order_key,
        header_id,
        align_flag,
        attribute33,
        bill_to_dept,
        ru_reported_tmip_quote_num,
        ru_ctmp_quote_num,
        invoice_print_format,
        order_origin,
        project_code,
        project_id,
        return_by_date,
        auto_cancel_date,
        foundation_flag,
        repair_order_role,
        ru_bk_repair_order_num_int,
        ru_src_reported_repair_ord_num,
        edw_create_user,
        edw_create_datetime,
        taxability,
        cisco_book_result_code,
        cisco_booked_date,
        cbn_flg,
        customer_acceptance_date,
        dpas_number,
        drp_ref_num,
        periodic_billing_flag,
        billing_schedule_cd_int,
        service_order_type_cd
    FROM {{ source('raw', 'wi_sales_order_v1_ext_cg1') }}
),

final AS (
    SELECT
        sales_order_key,
        header_id,
        align_flag,
        attribute33,
        bill_to_dept,
        ru_reported_tmip_quote_num,
        ru_ctmp_quote_num,
        invoice_print_format,
        order_origin,
        project_code,
        project_id,
        return_by_date,
        auto_cancel_date,
        foundation_flag,
        repair_order_role,
        ru_bk_repair_order_num_int,
        ru_src_reported_repair_ord_num,
        edw_create_user,
        edw_create_datetime,
        taxability,
        cisco_book_result_code,
        cisco_booked_date,
        cbn_flg,
        customer_acceptance_date,
        dpas_number,
        drp_ref_num,
        periodic_billing_flag,
        billing_schedule_cd_int,
        service_order_type_cd
    FROM source_wi_sales_order_v1_ext_cg1
)

SELECT * FROM final