{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_freight_line', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_FREIGHT_LINE',
        'target_table': 'N_AR_TRX_FREIGHT_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.851303+00:00'
    }
) }}

WITH 

source_w_ar_trx_freight_line AS (
    SELECT
        ar_trx_line_key,
        freight_line_description,
        unit_of_measure_code,
        ru_freight_debit_amount,
        ru_freight_credit_amount,
        bk_item_ar_trx_line_key,
        product_key,
        ru_ar_trx_key,
        freight_line_type,
        freight_line_cr_dr_type,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_trx_freight_line') }}
),

final AS (
    SELECT
        ar_trx_line_key,
        freight_line_description,
        unit_of_measure_code,
        ru_freight_debit_amount,
        ru_freight_credit_amount,
        bk_item_ar_trx_line_key,
        product_key,
        ru_ar_trx_key,
        freight_line_type,
        freight_line_cr_dr_type,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_w_ar_trx_freight_line
)

SELECT * FROM final