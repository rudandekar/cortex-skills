{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_dm_deal_financial_summary', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DM_DEAL_FINANCIAL_SUMMARY',
        'target_table': 'ST_INT_DM_DEAL_FINANCIAL_SUM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.943730+00:00'
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
    FROM source_st_int_dm_deal_financial_sum
)

SELECT * FROM final