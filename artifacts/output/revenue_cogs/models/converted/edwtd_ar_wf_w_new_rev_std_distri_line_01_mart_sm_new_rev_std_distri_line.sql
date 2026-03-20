{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_new_rev_std_distri_line', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_NEW_REV_STD_DISTRI_LINE',
        'target_table': 'SM_NEW_REV_STD_DISTRI_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.977156+00:00'
    }
) }}

WITH 

source_ex_xla_nrs_oa_incr AS (
    SELECT
        ae_header_id,
        ae_line_num,
        application_id,
        functional_curr_conversion_dt,
        gl_date,
        ledger_id,
        functional_credit_amt,
        functional_debit_amt,
        acct_class_cd,
        attribute3,
        sk_trx_distri_id_int,
        sk_trx_id_int,
        attribute7,
        offer_attrib_id_int,
        code_combination_id,
        currency_code,
        trxl_credit_amt,
        trxl_debit_amt,
        dist_type,
        accounting_rule_dur_cnt,
        bk_accounting_rule_name,
        accounting_rule_start_dt,
        accounting_rule_end_dt,
        order_quantity,
        attrib_pct,
        transaction_source,
        trx_class,
        customer_trx_id,
        sk_customer_trx_line_id_lint,
        deal_id,
        attribute4,
        attrib_cd,
        ss_cd,
        attribute13,
        bundle_type,
        parent_sub_sku_offset_id,
        parent_sub_sku_id,
        attribute2,
        exception_type
    FROM {{ source('raw', 'ex_xla_nrs_oa_incr') }}
),

source_w_new_rev_std_distri_line AS (
    SELECT
        new_rev_std_distri_line_key,
        sk_trx_distri_id_int,
        set_of_books_key,
        bk_company_cd,
        trxl_credit_amt,
        trxl_debit_amt,
        sk_ae_header_id_int,
        sk_ae_line_num_int,
        sk_trx_id_int,
        ar_trx_key,
        ar_trx_line_key,
        parent_ar_trx_line_key,
        functional_credit_amt,
        functional_debit_amt,
        gl_dt,
        trxl_curr_cd,
        functional_curr_conversion_dt,
        acct_class_cd,
        prdt_invoiced_qty,
        rev_distri_type_cd,
        general_ledger_acct_key,
        gl_posted_dt,
        bundle_prdt_key,
        sk_previous_trx_id_int,
        bk_deal_id,
        bk_accounting_rule_name,
        attrib_cd,
        accounting_rule_start_dt,
        accounting_rule_end_dt,
        accounting_rule_dur_cnt,
        offer_attrib_id_int,
        sales_territory_key,
        ar_batch_src_key,
        trx_class_cd,
        ss_cd,
        subscr_cd,
        attrib_pct,
        trx_src_cd,
        parent_offer_attrib_id_int,
        pob_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_new_rev_std_distri_line') }}
),

source_sm_new_rev_std_distri_line AS (
    SELECT
        xaas_offer_atrbtn_rev_line_key,
        sk_trx_distri_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_new_rev_std_distri_line') }}
),

source_wi_xla_nrs_oa_incr AS (
    SELECT
        ae_header_id,
        ae_line_num,
        application_id,
        functional_curr_conversion_dt,
        gl_date,
        ledger_id,
        functional_credit_amt,
        functional_debit_amt,
        acct_class_cd,
        attribute3,
        sk_trx_distri_id_int,
        sk_trx_id_int,
        attribute7,
        offer_attrib_id_int,
        code_combination_id,
        currency_code,
        trxl_credit_amt,
        trxl_debit_amt,
        dist_type,
        accounting_rule_dur_cnt,
        bk_accounting_rule_name,
        accounting_rule_start_dt,
        accounting_rule_end_dt,
        order_quantity,
        attrib_pct,
        transaction_source,
        trx_class,
        customer_trx_id,
        sk_customer_trx_line_id_lint,
        deal_id,
        attribute4,
        attrib_cd,
        ss_cd,
        attribute13,
        bundle_type,
        parent_sub_sku_offset_id,
        parent_sub_sku_id,
        attribute2
    FROM {{ source('raw', 'wi_xla_nrs_oa_incr') }}
),

final AS (
    SELECT
        xaas_offer_atrbtn_rev_line_key,
        sk_trx_distri_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_wi_xla_nrs_oa_incr
)

SELECT * FROM final