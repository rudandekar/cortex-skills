{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_deal_cust_details_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_DEAL_CUST_DETAILS_RNWL',
        'target_table': 'SM_DEAL_CUST_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.872852+00:00'
    }
) }}

WITH 

source_sm_deal_cust_details AS (
    SELECT
        deal_cust_details_key,
        bk_deal_id,
        cr_id,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_deal_cust_details') }}
),

final AS (
    SELECT
        deal_cust_details_key,
        bk_deal_id,
        cr_id,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_deal_cust_details
)

SELECT * FROM final