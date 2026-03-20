{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_prdt_rev_cost_adjstmnt_alctn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_PRDT_REV_COST_ADJSTMNT_ALCTN',
        'target_table': 'WI_RO_KEYS_REV_COST_ADJ_ALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.667906+00:00'
    }
) }}

WITH 

source_wi_ro_keys_rev_cost_adj_alc AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_ro_keys_rev_cost_adj_alc') }}
),

source_w_prdt_rev_cost_adjstmnt_alctn AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        sales_territory_key,
        product_key,
        bk_adjustment_measure_type_cd,
        bk_adj_sub_measure_type_cd,
        bk_adjustment_type_cd,
        bk_adjustment_company_name,
        iso_country_cd,
        adjstmnt_sales_subcoverage_cd,
        goods_product_key,
        adjustment_allocation_usd_amt,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        bk_deal_id
    FROM {{ source('raw', 'w_prdt_rev_cost_adjstmnt_alctn') }}
),

final AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM source_w_prdt_rev_cost_adjstmnt_alctn
)

SELECT * FROM final