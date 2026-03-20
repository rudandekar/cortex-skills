{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fdi_td_revenue_trx_ff', 'batch', 'edwtd_ea'],
    meta={
        'source_workflow': 'wf_m_ST_FDI_TD_REVENUE_TRX_FF',
        'target_table': 'ST_FDI_TD_REVENUE_TRX_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.897302+00:00'
    }
) }}

WITH 

source_ff_fdi_td_revenue_trx_ff AS (
    SELECT
        source_entity_name,
        dv_fiscal_year_month_number_int,
        adjustment_type_code,
        service_flag,
        product_id,
        sales_territory_name_code,
        revenue_cogs_type_code,
        extended_net_cost_usd_amt,
        extended_net_revenue_usd_amt,
        dv_shipment_revenue_usd_amt
    FROM {{ source('raw', 'ff_fdi_td_revenue_trx_ff') }}
),

final AS (
    SELECT
        source_entity_name,
        dv_fiscal_year_month_number_int,
        adjustment_type_code,
        service_flag,
        product_id,
        sales_territory_name_code,
        revenue_cogs_type_code,
        extended_net_cost_usd_amt,
        extended_net_revenue_usd_amt,
        dv_shipment_revenue_usd_amt
    FROM source_ff_fdi_td_revenue_trx_ff
)

SELECT * FROM final