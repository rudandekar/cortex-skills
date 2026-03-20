{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_nr_trx', 'batch', 'edwtd_mkt_cic'],
    meta={
        'source_workflow': 'wf_m_WI_NR_TRX',
        'target_table': 'WI_NR_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.776809+00:00'
    }
) }}

WITH 

source_st_erp_nr_trx AS (
    SELECT
        advanced_technology_name,
        dd_customer_party_key,
        hq_customer_party_key,
        fiscal_year_quarter_number_int,
        transaction_type,
        sales_territory_key,
        partner_site_party_key,
        dd_comp_us_net_price_amount
    FROM {{ source('raw', 'st_erp_nr_trx') }}
),

final AS (
    SELECT
        advanced_technology_name,
        dd_customer_party_key,
        hq_customer_party_key,
        fiscal_year_quarter_number_int,
        dd_comp_us_net_price_amount,
        new_repeat_flg,
        new_repeat_tms_flg,
        last_purchase_flg,
        last_purchase_tms_flg,
        new_repeat_hq_flg,
        new_repeat_tms_hq_flg,
        last_purchase_hq_flg,
        last_purchase_tms_hq_flg
    FROM source_st_erp_nr_trx
)

SELECT * FROM final