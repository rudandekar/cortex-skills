{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_receiving_transaction', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_RECEIVING_TRANSACTION',
        'target_table': 'N_RECEIVING_TRANSACTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.030130+00:00'
    }
) }}

WITH 

source_w_receiving_transaction AS (
    SELECT
        receiving_transaction_key,
        unit_price_transactional_amt,
        override_conversion_rt,
        parent_role,
        bk_transaction_dtm,
        dv_transaction_dt,
        bk_transaction_type_cd,
        sk_transaction_id_int,
        ss_cd,
        bk_receipt_shipment_line_key,
        src_document_transaction_qty,
        primary_transaction_qty,
        po_shipment_distribution_key,
        primary_unit_of_measure_cd,
        src_dcumnt_unit_of_measure_cd,
        transactional_currency_cd,
        gl_posted_flg,
        ru_parent_receiving_trx_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_receiving_transaction') }}
),

final AS (
    SELECT
        receiving_transaction_key,
        unit_price_transactional_amt,
        override_conversion_rt,
        parent_role,
        bk_transaction_dtm,
        dv_transaction_dt,
        bk_transaction_type_cd,
        sk_transaction_id_int,
        ss_cd,
        bk_receipt_shipment_line_key,
        src_document_transaction_qty,
        primary_transaction_qty,
        po_shipment_distribution_key,
        primary_unit_of_measure_cd,
        src_dcumnt_unit_of_measure_cd,
        transactional_currency_cd,
        gl_posted_flg,
        ru_parent_receiving_trx_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        destination_type_cd
    FROM source_w_receiving_transaction
)

SELECT * FROM final