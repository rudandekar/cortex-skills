{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sal_nrt_retro', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SAL_NRT_RETRO',
        'target_table': 'WI_NSAL_NRT_RETRO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.699855+00:00'
    }
) }}

WITH 

source_n_sales_adjustment_line_nrt AS (
    SELECT
        bk_sales_adj_line_number_int,
        bk_sales_adj_number_int,
        sales_adjustment_datetime,
        cost_dollar_adj_amt,
        base_price_dollar_adj_amt,
        selling_price_dollar_adj_amt,
        ide_ifc_adj_initiator_name,
        ide_ifc_adj_approver_name,
        ide_ifc_adj_approval_date,
        ide_ifc_comment_1_txt,
        ide_ifc_comment_2_txt,
        ide_ifc_comment_3_txt,
        ide_ifc_comment_4_txt,
        ide_ifc_comment_5_txt,
        ide_ifc_description_txt,
        ide_ifc_adj_reporting_type_cd,
        ide_ifc_ide_reported_deal_id,
        ide_ifc_ide_rptd_prod_type_cd,
        dv_channel_booking_flag,
        sales_territory_key,
        ru_bk_technology_group_id,
        ru_bk_business_unit_id,
        ru_bk_product_family_id,
        ru_bk_sales_rep_number,
        ru_bk_customer_account_number,
        ru_bk_sales_channel_code,
        ru_bk_pos_transaction_id,
        ru_bk_sales_cr_type_code,
        ru_bk_sales_cr_type_origin_cd,
        ru_bk_pos_transaction_id_int,
        ru_bk_poscra_sales_rep_number,
        product_hierarchy_type,
        pos_sales_credit_role,
        sales_representative_role,
        sales_order_role,
        pos_role,
        customer_role,
        sales_channel_role,
        ss_code,
        service_booking_flag,
        sales_adjustment_date,
        bk_fiscal_calendar_code,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        dv_fiscal_year_mth_number_int,
        process_date,
        reported_sales_order_num_lint,
        sales_order_key,
        product_key,
        distributor_offset_flg,
        submitted_by_name,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        sales_adj_line_created_dtm,
        managed_services_role,
        ru_ms_transaction_group_cd,
        ru_ms_deal_idntfctn_method_cd,
        ru_src_rptd_ms_partner_id_int,
        ru_partner_party_key,
        dv_service_category_cd,
        service_type_cd,
        manual_trx_key,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dv_corporate_booking_flg
    FROM {{ source('raw', 'n_sales_adjustment_line_nrt') }}
),

final AS (
    SELECT
        bk_sales_adj_line_number_int,
        sales_territory_key,
        sh_territory_id,
        data_source,
        ru_bk_sales_rep_number,
        sh_salesrep_id
    FROM source_n_sales_adjustment_line_nrt
)

SELECT * FROM final