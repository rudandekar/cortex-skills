{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_chnl_ret_dvad_part_country', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_CHNL_RET_DVAD_PART_COUNTRY',
        'target_table': 'WI_CHNL_RET_DVAD_PARTNERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.535735+00:00'
    }
) }}

WITH 

source_wi_chnl_ret_dvad_partners AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk
    FROM {{ source('raw', 'wi_chnl_ret_dvad_partners') }}
),

source_wi_chnl_ret_dvad_patterns AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key
    FROM {{ source('raw', 'wi_chnl_ret_dvad_patterns') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        ship_to_customer_key
    FROM source_wi_chnl_ret_dvad_patterns
)

SELECT * FROM final