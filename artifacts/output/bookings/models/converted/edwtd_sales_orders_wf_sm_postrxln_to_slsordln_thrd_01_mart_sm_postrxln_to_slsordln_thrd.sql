{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_postrxln_to_slsordln_thrd', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_POSTRXLN_TO_SLSORDLN_THRD',
        'target_table': 'SM_POSTRXLN_TO_SLSORDLN_THRD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.085603+00:00'
    }
) }}

WITH 

source_sm_postrxln_to_slsordln_thrd AS (
    SELECT
        postrxln_to_slsordln_thrd_key,
        bk_pos_transaction_id_int,
        sales_motion_cd,
        sales_order_line_key,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_postrxln_to_slsordln_thrd') }}
),

final AS (
    SELECT
        postrxln_to_slsordln_thrd_key,
        bk_pos_transaction_id_int,
        sales_motion_cd,
        sales_order_line_key,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_postrxln_to_slsordln_thrd
)

SELECT * FROM final