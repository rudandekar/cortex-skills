{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_trnglted_gl_svc_rev_aloc', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_TRNGLTED_GL_SVC_REV_ALOC',
        'target_table': 'WI_RO_KEYS_TRN_GL_SVC_REV_ALLC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.831209+00:00'
    }
) }}

WITH 

source_wi_ro_keys_trn_gl_svc_rev_allc AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_ro_keys_trn_gl_svc_rev_allc') }}
),

source_w_trnglted_gl_svc_rev_allocatn AS (
    SELECT
        trngtd_gl_svc_rev_alctn_key,
        bk_corporate_revenue_flg,
        bk_rev_measure_trnsctn_type_cd,
        bk_transaction_type_categry_cd,
        bk_ic_revenue_flg,
        bk_revenue_flg,
        bk_charges_flg,
        bk_misc_flg,
        bk_service_flg,
        bk_international_demo_flg,
        bk_replacement_demo_flg,
        bk_trngltd_sales_territory_key,
        bk_sales_territory_key,
        bk_product_key,
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        bk_financial_account_cd,
        bk_direct_corp_adj_type_cd,
        comp_us_net_price_amt,
        comp_us_net_list_price_amt,
        comp_us_gross_list_price_am,
        comp_us_net_cost_amt,
        comp_us_gross_rev_amt,
        comp_us_net_rev_amt,
        comp_us_2tier_cmdm_amt,
        comp_us_gross_cost_amt,
        comp_us_stndrd_price_amt,
        bk_fiscal_year_mth_number_int,
        triangulation_type_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_prdt_subgroup_alloc_src_cd,
        sales_order_line_key,
        bk_deal_id,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_trnglted_gl_svc_rev_allocatn') }}
),

final AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM source_w_trnglted_gl_svc_rev_allocatn
)

SELECT * FROM final