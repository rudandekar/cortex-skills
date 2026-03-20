{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sls_ord_ln_shpmnt_dlvry_evnt', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SLS_ORD_LN_SHPMNT_DLVRY_EVNT',
        'target_table': 'N_SLS_ORD_LN_SHPMNT_DLVRY_EVNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.781993+00:00'
    }
) }}

WITH 

source_w_sls_ord_ln_shpmnt_dlvry_evnt AS (
    SELECT
        sol_shipment_delivery_key,
        bk_shipset_num_int,
        sales_order_key,
        bk_event_type_cd,
        freight_carrier_id,
        all_milestones_rcvd_seq_flg,
        bk_event_dtm,
        dv_event_dt,
        expected_arrival_dtm,
        dv_expected_arrival_dt,
        flight_num,
        house_airwaybill_num,
        master_airwaybill_num,
        piece_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sequence_status_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sls_ord_ln_shpmnt_dlvry_evnt') }}
),

final AS (
    SELECT
        sol_shipment_delivery_key,
        bk_shipset_num_int,
        sales_order_key,
        bk_event_type_cd,
        freight_carrier_id,
        all_milestones_rcvd_seq_flg,
        bk_event_dtm,
        dv_event_dt,
        expected_arrival_dtm,
        dv_expected_arrival_dt,
        flight_num,
        house_airwaybill_num,
        master_airwaybill_num,
        piece_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sequence_status_cd
    FROM source_w_sls_ord_ln_shpmnt_dlvry_evnt
)

SELECT * FROM final