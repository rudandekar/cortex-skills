{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sr_ssc_cost_measure_rtm_updt', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_SR_SSC_COST_MEASURE_RTM_UPDT',
        'target_table': 'WI_CCM_SSC_KEYS_CH_FLG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.832289+00:00'
    }
) }}

WITH 

source_wi_ccm_ssc_keys_rtm AS (
    SELECT
        service_request_ssc_cost_key,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        src_rptd_service_contarct_num,
        channel_flg,
        dv_route_to_market_cd
    FROM {{ source('raw', 'wi_ccm_ssc_keys_rtm') }}
),

source_wi_ccm_ssc_keys AS (
    SELECT
        service_request_ssc_cost_key,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        src_rptd_service_contarct_num
    FROM {{ source('raw', 'wi_ccm_ssc_keys') }}
),

source_wi_ccm_ssc_keys_ch_flg AS (
    SELECT
        service_request_ssc_cost_key,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        src_rptd_service_contarct_num,
        channel_flg
    FROM {{ source('raw', 'wi_ccm_ssc_keys_ch_flg') }}
),

final AS (
    SELECT
        service_request_ssc_cost_key,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        src_rptd_service_contarct_num,
        channel_flg
    FROM source_wi_ccm_ssc_keys_ch_flg
)

SELECT * FROM final