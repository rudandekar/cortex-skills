{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pol_shipment_revision', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_POL_SHIPMENT_REVISION',
        'target_table': 'N_POL_SHIPMENT_REVISION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.952162+00:00'
    }
) }}

WITH 

source_w_pol_shipment_revision AS (
    SELECT
        pol_shipment_key,
        bk_po_revision_num_int,
        purchase_order_key,
        po_line_shipment_need_dt,
        current_record_flg,
        po_line_shipment_status_cd,
        pd_po_line_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_pol_shipment_revision') }}
),

final AS (
    SELECT
        pol_shipment_key,
        bk_po_revision_num_int,
        purchase_order_key,
        po_line_shipment_need_dt,
        current_record_flg,
        po_line_shipment_status_cd,
        pd_po_line_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_pol_shipment_revision
)

SELECT * FROM final