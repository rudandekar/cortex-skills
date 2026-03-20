{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_deal_fulfillment_distributor', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_DEAL_FULFILLMENT_DISTRIBUTOR',
        'target_table': 'SM_DEAL_FULFILLMENT_DISTRIBUTOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.888512+00:00'
    }
) }}

WITH 

source_sm_deal_fulfillment_distributor AS (
    SELECT
        deal_fulfillment_distri_key,
        sk_object_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_deal_fulfillment_distributor') }}
),

final AS (
    SELECT
        deal_fulfillment_distri_key,
        sk_object_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_deal_fulfillment_distributor
)

SELECT * FROM final