{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_xxcca_oe_ord_headers', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_MF_XXCCA_OE_ORD_HEADERS',
        'target_table': 'ST_MF_XXCCA_OE_ORD_HEADERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.438783+00:00'
    }
) }}

WITH 

source_ff_mf_xxcca_oe_ord_headers AS (
    SELECT
        header_id,
        cms_source_code,
        cms_source_header_id,
        creation_date,
        last_update_date,
        batch_id,
        action_code,
        create_datetime
    FROM {{ source('raw', 'ff_mf_xxcca_oe_ord_headers') }}
),

final AS (
    SELECT
        batch_id,
        create_datetime,
        header_id,
        cms_source_code,
        cms_source_header_id,
        creation_date,
        last_update_date,
        action_code
    FROM source_ff_mf_xxcca_oe_ord_headers
)

SELECT * FROM final