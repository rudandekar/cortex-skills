{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_appl_ar_trx_line_assgn', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_APPL_AR_TRX_LINE_ASSGN',
        'target_table': 'N_APPL_AR_TRX_LINE_ASSGN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.422061+00:00'
    }
) }}

WITH 

source_w_appl_ar_trx_line_assgn AS (
    SELECT
        sales_order_line_key,
        ar_trx_line_key,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code
    FROM {{ source('raw', 'w_appl_ar_trx_line_assgn') }}
),

final AS (
    SELECT
        sales_order_line_key,
        ar_trx_line_key,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_w_appl_ar_trx_line_assgn
)

SELECT * FROM final