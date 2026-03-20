{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_report_line_travel', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_REPORT_LINE_TRAVEL',
        'target_table': 'N_EXPENSE_REPORT_LINE_TRAVEL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.167002+00:00'
    }
) }}

WITH 

source_w_expense_report_line_travel AS (
    SELECT
        er_line_from_location_descr,
        er_line_to_location_descr,
        er_line_passenger_cnt,
        er_line_mileage_rt_adjstd_flg,
        er_line_fuel_type_cd,
        er_line_unit_of_distance_cd,
        er_line_vehicle_category_cd,
        er_line_license_plate_num,
        er_line_ttl_trip_distance_amt,
        er_line_mile_dly_distance_amt,
        er_line_mile_dly_money_amt,
        er_line_mile_reimbursement_rt,
        expense_report_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_expense_report_line_travel') }}
),

final AS (
    SELECT
        er_line_from_location_descr,
        er_line_to_location_descr,
        er_line_passenger_cnt,
        er_line_mileage_rt_adjstd_flg,
        er_line_fuel_type_cd,
        er_line_unit_of_distance_cd,
        er_line_vehicle_category_cd,
        er_line_license_plate_num,
        er_line_ttl_trip_distance_amt,
        er_line_mile_dly_distance_amt,
        er_line_mile_dly_money_amt,
        er_line_mile_reimbursement_rt,
        expense_report_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_expense_report_line_travel
)

SELECT * FROM final