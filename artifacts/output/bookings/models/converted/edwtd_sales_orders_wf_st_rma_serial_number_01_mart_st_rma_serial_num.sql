{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_rma_serial_num', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_RMA_SERIAL_NUM',
        'target_table': 'ST_RMA_SERIAL_NUM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.056527+00:00'
    }
) }}

WITH 

source_ff_rma_serial_num AS (
    SELECT
        line_id,
        rma_serial_number,
        creation_date,
        creation_by,
        last_updated_by,
        last_updated_date
    FROM {{ source('raw', 'ff_rma_serial_num') }}
),

final AS (
    SELECT
        line_id,
        rma_serial_num,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date
    FROM source_ff_rma_serial_num
)

SELECT * FROM final