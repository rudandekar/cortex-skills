{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_receipt_shipment', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_RECEIPT_SHIPMENT',
        'target_table': 'N_RECEIPT_SHIPMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.888257+00:00'
    }
) }}

WITH 

source_w_receipt_shipment AS (
    SELECT
        receipt_shipment_key,
        shipment_num,
        bk_carrier_id,
        ru_intrnl_src_invntry_orgn_key,
        receipt_num,
        expected_receipt_dtm,
        waybill_num,
        internal_source_role,
        source_deleted_flg,
        sk_shipment_header_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_receipt_shipment') }}
),

final AS (
    SELECT
        receipt_shipment_key,
        shipment_num,
        bk_carrier_id,
        ru_intrnl_src_invntry_orgn_key,
        receipt_num,
        expected_receipt_dtm,
        waybill_num,
        internal_source_role,
        source_deleted_flg,
        sk_shipment_header_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_receipt_shipment
)

SELECT * FROM final