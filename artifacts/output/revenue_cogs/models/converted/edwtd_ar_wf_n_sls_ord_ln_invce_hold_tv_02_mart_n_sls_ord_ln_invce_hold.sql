{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sls_ord_ln_invce_hold_tv', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_SLS_ORD_LN_INVCE_HOLD_TV',
        'target_table': 'N_SLS_ORD_LN_INVCE_HOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.813505+00:00'
    }
) }}

WITH 

source_w_sls_ord_ln_invce_hold AS (
    SELECT
        bk_sales_order_line_key,
        invoice_hold_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sls_ord_ln_invce_hold') }}
),

source_n_sls_ord_ln_invce_hold_tv AS (
    SELECT
        bk_sales_order_line_key,
        invoice_hold_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'n_sls_ord_ln_invce_hold_tv') }}
),

final AS (
    SELECT
        bk_sales_order_line_key,
        invoice_hold_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_sls_ord_ln_invce_hold_tv
)

SELECT * FROM final