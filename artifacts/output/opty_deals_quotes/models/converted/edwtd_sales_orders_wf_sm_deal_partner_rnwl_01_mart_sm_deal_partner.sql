{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_deal_partner_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_DEAL_PARTNER_RNWL',
        'target_table': 'SM_DEAL_PARTNER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.937512+00:00'
    }
) }}

WITH 

source_sm_deal_partner AS (
    SELECT
        deal_partner_key,
        sk_deal_partner_object_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_deal_partner') }}
),

final AS (
    SELECT
        deal_partner_key,
        sk_deal_partner_object_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_deal_partner
)

SELECT * FROM final