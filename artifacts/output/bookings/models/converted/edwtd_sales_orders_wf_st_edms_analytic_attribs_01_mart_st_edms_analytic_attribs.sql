{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_edms_analytic_attribs', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_EDMS_ANALYTIC_ATTRIBS',
        'target_table': 'ST_EDMS_ANALYTIC_ATTRIBS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.410558+00:00'
    }
) }}

WITH 

source_ff_edms_analytic_attribs AS (
    SELECT
        request_num,
        request_ver,
        contract_number,
        discount_id,
        creation_date
    FROM {{ source('raw', 'ff_edms_analytic_attribs') }}
),

final AS (
    SELECT
        request_num,
        request_ver,
        contract_number,
        discount_id,
        creation_date
    FROM source_ff_edms_analytic_attribs
)

SELECT * FROM final