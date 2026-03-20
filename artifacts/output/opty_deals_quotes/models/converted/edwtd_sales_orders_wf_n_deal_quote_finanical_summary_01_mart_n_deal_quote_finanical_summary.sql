{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_quote_finanical_summary', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUOTE_FINANICAL_SUMMARY',
        'target_table': 'N_DEAL_QUOTE_FINANICAL_SUMMARY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.916587+00:00'
    }
) }}

WITH 

source_w_deal_quote_finanical_summary AS (
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
    FROM {{ source('raw', 'w_deal_quote_finanical_summary') }}
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
        edw_update_user
    FROM source_w_deal_quote_finanical_summary
)

SELECT * FROM final