{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_order_headers_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_ORDER_HEADERS_ALL',
        'target_table': 'ST_OOD_FUSN_ORDER_HEADERS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.030809+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_order_headers_all AS (
    SELECT
        header_id,
        org_id,
        order_number,
        customer_acceptance_flag,
        creation_date,
        last_update_date,
        split_key,
        action_code,
        create_datetime
    FROM {{ source('raw', 'ff_ood_fusn_order_headers_all') }}
),

final AS (
    SELECT
        header_id,
        org_id,
        order_number,
        customer_acceptance_flag,
        creation_date,
        last_update_date,
        split_key,
        action_code,
        create_datetime
    FROM source_ff_ood_fusn_order_headers_all
)

SELECT * FROM final