{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_deal_fulfillment_distributor_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_DEAL_FULFILLMENT_DISTRIBUTOR_RNWL',
        'target_table': 'SM_DEAL_FULFILLMENT_DISTRIBUTOR_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.881271+00:00'
    }
) }}

WITH 

source_sm_deal_fulfillment_distributor_rnwl AS (
    SELECT
        deal_fulfillment_distri_key,
        sk_object_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_deal_fulfillment_distributor_rnwl') }}
),

final AS (
    SELECT
        deal_fulfillment_distri_key,
        sk_object_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_deal_fulfillment_distributor_rnwl
)

SELECT * FROM final