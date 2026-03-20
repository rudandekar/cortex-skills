{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sls_ord_ln_shpmnt_dlvry_evnt', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_SLS_ORD_LN_SHPMNT_DLVRY_EVNT',
        'target_table': 'W_SLS_ORD_LN_SHPMNT_DLVRY_EVNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.411807+00:00'
    }
) }}

WITH 

source_st_cg1_x_dv_3b3_shpstat_hd_ifc AS (
    SELECT
        b2b_message_id,
        message_priority,
        partner_name,
        status_date,
        order_number,
        ship_set_number,
        origin_airport_code,
        destination_airport_code,
        delivery_id,
        country_exp_imp,
        expected_arrival_date,
        status_met_date,
        status_code,
        city_name,
        country_name,
        state_name,
        master_airway_bill,
        house_airway_bill,
        carrier_reference,
        flight_number,
        number_of_pieces,
        record_status,
        error_message,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        concurrent_program_request,
        id,
        rrn_exists_flag,
        domain_name,
        otm_interface_status,
        retry_count,
        otm_transmission_number,
        otm_transmission_errors,
        notified_to_carrier,
        counter_value,
        sequence_status,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_x_dv_3b3_shpstat_hd_ifc') }}
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
        sequence_status_cd,
        action_code,
        dml_type
    FROM source_st_cg1_x_dv_3b3_shpstat_hd_ifc
)

SELECT * FROM final