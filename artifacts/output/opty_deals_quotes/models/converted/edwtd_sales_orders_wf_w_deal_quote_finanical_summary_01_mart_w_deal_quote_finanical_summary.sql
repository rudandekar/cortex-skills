{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_quote_finanical_summary', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_QUOTE_FINANICAL_SUMMARY',
        'target_table': 'W_DEAL_QUOTE_FINANICAL_SUMMARY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.977079+00:00'
    }
) }}

WITH 

source_st_int_dm_deal_financial_sum AS (
    SELECT
        deal_id,
        object_id,
        reference_object_id,
        reference_object_type,
        summary_type,
        ext_list_price,
        discount_percent,
        ext_net_price,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        total_cost,
        gross_margin,
        gross_margin_percent,
        standard_cost,
        overhead_cost,
        royalty_cost,
        standard_margin,
        standard_margin_percent,
        negotiated_net_price,
        negotiated_discount,
        net_price_excl_trade_in,
        discount_excl_trade_in,
        net_price_excl_credits,
        edw_updated_date,
        ss_code
    FROM {{ source('raw', 'st_int_dm_deal_financial_sum') }}
),

final AS (
    SELECT
        bk_quote_num,
        bk_summary_type_cd,
        ttl_extended_list_prc_usd_amt,
        ttl_extended_net_price_usd_amt,
        total_discount_percent,
        total_cost_usd_amt,
        standard_cost_usd_amt,
        overhead_cost_usd_amt,
        royalty_cost_usd_amt,
        gross_margin_percent,
        gross_margin_usd_amt,
        standard_margin_usd_amt,
        standard_margin_percent,
        negotiated_net_price_usd_amt,
        negotiated_discount_percent,
        dscnt_excldng_trade_in_usd_amt,
        net_prc_excldng_trd_in_usd_amt,
        net_prc_excldng_crdts_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_int_dm_deal_financial_sum
)

SELECT * FROM final