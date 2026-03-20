{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rma_serial_number', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_RMA_SERIAL_NUMBER',
        'target_table': 'N_RMA_SERIAL_NUMBER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.235519+00:00'
    }
) }}

WITH 

source_w_rma_serial_number AS (
    SELECT
        line_id,
        rma_serial_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'w_rma_serial_number') }}
),

final AS (
    SELECT
        rma_return_sol_key,
        bk_rma_serial_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_rma_serial_number
)

SELECT * FROM final