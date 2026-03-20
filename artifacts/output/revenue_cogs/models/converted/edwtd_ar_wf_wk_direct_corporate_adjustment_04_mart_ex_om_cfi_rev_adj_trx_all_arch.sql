{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_direct_corporate_adjustment', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_DIRECT_CORPORATE_ADJUSTMENT',
        'target_table': 'EX_OM_CFI_REV_ADJ_TRX_ALL_ARCH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.218054+00:00'
    }
) }}

WITH 

source_w_direct_corporate_adjustment AS (
    SELECT
        direct_corp_adjustment_key,
        sales_order_line_key,
        dv_transaction_dt,
        transaction_dtm,
        adjusment_cogs_usd_amt,
        adjusment_cogs_functional_amt,
        adjustment_revenue_usd_amt,
        adjustment_revenue_functnl_amt,
        bk_direct_corp_adj_type_cd,
        direct_corp_adjustment_qty,
        transactional_currency_cd,
        bk_company_cd,
        set_of_books_key,
        sk_adjustment_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        sales_order_key,
        bk_offer_atrbtn_id_int,
        bk_atrbtn_source_sys_cd,
        dv_atrbtn_parent_slsord_ln_key,
        attribution_cd,
        product_key,
        ru_attribution_pct,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_direct_corporate_adjustment') }}
),

source_ex_cg_cfi_rev_adj_trx_all AS (
    SELECT
        ae_header_id,
        ae_line_num,
        application_id,
        code_combination_id,
        gl_sl_link_id,
        accounting_class_code,
        party_id,
        party_site_id,
        party_type_code,
        entered_dr,
        entered_cr,
        accounted_dr,
        accounted_cr,
        currency_code,
        attribute_category,
        order_number,
        order_line_id,
        trx_number,
        trx_line_id,
        trx_date,
        transaction_dist_id,
        dist_type,
        order_quantity,
        expiration_date,
        gl_sl_link_table,
        unrounded_accounted_dr,
        unrounded_accounted_cr,
        unrounded_entered_dr,
        unrounded_entered_cr,
        gl_date,
        ledger_id,
        source_table,
        source_id,
        creation_date,
        last_update_date,
        global_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        exception_type,
        attribution_cd,
        attribution_pct,
        sku,
        oa_attrib_id,
        transaction_id,
        attribute2
    FROM {{ source('raw', 'ex_cg_cfi_rev_adj_trx_all') }}
),

source_sm_direct_corporate_adjustment AS (
    SELECT
        direct_corp_adjustment_key,
        sk_adjustment_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_direct_corporate_adjustment') }}
),

source_wi_xla_ae_line_corp_adj_incr AS (
    SELECT
        ae_header_id,
        ae_line_num,
        application_id,
        code_combination_id,
        gl_sl_link_id,
        accounting_class_code,
        party_id,
        party_site_id,
        party_type_code,
        entered_dr,
        entered_cr,
        accounted_dr,
        accounted_cr,
        currency_code,
        attribute_category,
        order_number,
        order_line_id,
        trx_number,
        trx_line_id,
        trx_date,
        transaction_dist_id,
        dist_type,
        order_quantity,
        expiration_date,
        gl_sl_link_table,
        unrounded_accounted_dr,
        unrounded_accounted_cr,
        unrounded_entered_dr,
        unrounded_entered_cr,
        gl_date,
        ledger_id,
        source_table,
        source_id,
        creation_date,
        last_update_date,
        global_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        attribution_cd,
        attribution_pct,
        sku,
        oa_attrib_id,
        transaction_id,
        attribute2
    FROM {{ source('raw', 'wi_xla_ae_line_corp_adj_incr') }}
),

source_st_om_cfi_rev_adj_trx_all_rev AS (
    SELECT
        batch_id,
        global_name,
        rev_adj_trx_id,
        adj_type_id,
        adj_source_type,
        source_header_id,
        source_line_id,
        currency_code,
        adjustment_revenue,
        adjustment_revenue_usd,
        adjustment_cogs,
        adjustment_cogs_usd,
        period_year,
        period_num,
        rev_summary_id,
        org_id,
        set_of_books_id,
        quantity_invoiced,
        csm_flag,
        transaction_date,
        adj_cat_id,
        allocation_method,
        db_source_name,
        ges_update_date,
        create_datetime,
        action_code,
        attribute8,
        attribute9
    FROM {{ source('raw', 'st_om_cfi_rev_adj_trx_all_rev') }}
),

source_sm_sales_order AS (
    SELECT
        sales_order_key,
        bk_company_code,
        bk_set_of_books_key,
        bk_so_number_int,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_sales_order') }}
),

source_ev_op_unit_gaap_company AS (
    SELECT
        sk_organization_id_int,
        bk_company_code,
        set_of_books_key,
        bk_operating_unit_name_code
    FROM {{ source('raw', 'ev_op_unit_gaap_company') }}
),

source_ex_om_cfi_rev_adj_trx_all AS (
    SELECT
        batch_id,
        global_name,
        rev_adj_trx_id,
        adj_type_id,
        adj_source_type,
        source_header_id,
        source_line_id,
        currency_code,
        adjustment_revenue,
        adjustment_revenue_usd,
        adjustment_cogs,
        adjustment_cogs_usd,
        period_year,
        period_num,
        rev_summary_id,
        org_id,
        set_of_books_id,
        quantity_invoiced,
        csm_flag,
        transaction_date,
        adj_cat_id,
        allocation_method,
        db_source_name,
        ges_update_date,
        create_datetime,
        action_code,
        exception_type,
        attribute8,
        attribute9
    FROM {{ source('raw', 'ex_om_cfi_rev_adj_trx_all') }}
),

source_n_direct_corp_adj_type AS (
    SELECT
        bk_direct_corp_adj_type_cd,
        direct_corp_adj_end_dtm,
        direct_corp_adj_start_dtm,
        direct_corp_adj_descr,
        sk_adj_type_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_direct_corp_adj_type') }}
),

source_sm_direct_corporate_adjustment AS (
    SELECT
        direct_corp_adjustment_key,
        sk_adjustment_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_direct_corporate_adjustment') }}
),

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

source_sm_sales_order_line AS (
    SELECT
        sales_order_line_key,
        ss_code,
        sk_so_line_id_int,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_sales_order_line') }}
),

final AS (
    SELECT
        batch_id,
        global_name,
        rev_adj_trx_id,
        adj_type_id,
        adj_source_type,
        source_header_id,
        source_line_id,
        currency_code,
        adjustment_revenue,
        adjustment_revenue_usd,
        adjustment_cogs,
        adjustment_cogs_usd,
        period_year,
        period_num,
        rev_summary_id,
        org_id,
        set_of_books_id,
        quantity_invoiced,
        csm_flag,
        transaction_date,
        adj_cat_id,
        allocation_method,
        db_source_name,
        ges_update_date,
        create_datetime,
        action_code,
        attribute8,
        attribute9
    FROM source_sm_sales_order_line
)

SELECT * FROM final