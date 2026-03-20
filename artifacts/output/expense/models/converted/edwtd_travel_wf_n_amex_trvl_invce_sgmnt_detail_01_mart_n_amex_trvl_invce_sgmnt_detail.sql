{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_amex_trvl_invce_sgmnt_detail', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_N_AMEX_TRVL_INVCE_SGMNT_DETAIL',
        'target_table': 'N_AMEX_TRVL_INVCE_SGMNT_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.655985+00:00'
    }
) }}

WITH 

source_w_amex_trvl_invce_sgmnt_detail AS (
    SELECT
        bk_amex_travel_invoice_num,
        bk_amex_travel_invoice_dt,
        bk_amex_trvl_inv_psngr_num_int,
        bk_amx_trvl_inv_actg_brnch_num,
        bk_amex_trvl_inv_machine_id,
        bk_invoice_segment_seq_num_int,
        bk_class_of_service_type_cd,
        segment_arrival_dtm,
        segment_departure_dtm,
        segment_connection_flg,
        reported_currency_exchange_rt,
        segment_domestic_intrntnl_cd,
        segment_miles_cnt,
        segment_fare_reporting_amt,
        segment_type,
        segment_origin_iso_country_cd,
        segment_dest_iso_country_cd,
        fare_type_cd,
        flight_fare_basis_cd,
        ru_flight_orgn_iata_airport_cd,
        ru_passage_dstn_rail_stn_cd,
        ru_passage_orgn_rail_stn_cd,
        ru_flight_dstn_iata_airport_cd,
        ru_trip_origin_rail_stn_cd,
        ru_trip_orgn_iata_airport_cd,
        ru_flight_num,
        ru_flight_service_class_cd,
        ru_iata_airline_cd,
        ru_train_num,
        ru_rail_service_class_cd,
        ru_amex_rail_carrier_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        flight_departure_dt,
        flight_arrival_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_amex_trvl_invce_sgmnt_detail') }}
),

final AS (
    SELECT
        bk_amex_travel_invoice_num,
        bk_amex_travel_invoice_dt,
        bk_amex_trvl_inv_psngr_num_int,
        bk_amx_trvl_inv_actg_brnch_num,
        bk_amex_trvl_inv_machine_id,
        bk_invoice_segment_seq_num_int,
        bk_class_of_service_type_cd,
        segment_arrival_dtm,
        segment_departure_dtm,
        segment_connection_flg,
        reported_currency_exchange_rt,
        segment_domestic_intrntnl_cd,
        segment_miles_cnt,
        segment_fare_reporting_amt,
        segment_type,
        segment_origin_iso_country_cd,
        segment_dest_iso_country_cd,
        fare_type_cd,
        flight_fare_basis_cd,
        ru_flight_orgn_iata_airport_cd,
        ru_passage_dstn_rail_stn_cd,
        ru_passage_orgn_rail_stn_cd,
        ru_flight_dstn_iata_airport_cd,
        ru_trip_origin_rail_stn_cd,
        ru_trip_orgn_iata_airport_cd,
        ru_flight_num,
        ru_flight_service_class_cd,
        ru_iata_airline_cd,
        ru_train_num,
        ru_rail_service_class_cd,
        ru_amex_rail_carrier_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        flight_departure_dt,
        flight_arrival_dt
    FROM source_w_amex_trvl_invce_sgmnt_detail
)

SELECT * FROM final