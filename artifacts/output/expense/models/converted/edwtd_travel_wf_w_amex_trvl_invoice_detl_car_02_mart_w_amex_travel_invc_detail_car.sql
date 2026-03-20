{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_amex_trvl_invoice_detl_car', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_W_AMEX_TRVL_INVOICE_DETL_CAR',
        'target_table': 'W_AMEX_TRAVEL_INVC_DETAIL_CAR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.633121+00:00'
    }
) }}

WITH 

source_st_post_travel_car AS (
    SELECT
        batch_id,
        tcidte,
        tcmcid,
        tcacbr,
        tcinv,
        tcnpas,
        tcseqn,
        tctype,
        tcacde,
        tcpdte,
        tcploc,
        tccity,
        tcstat,
        tcazip,
        tcpcty,
        tcday,
        tcncar,
        tctcar,
        tcrate,
        tcreas,
        tcccur,
        tcoamt,
        tccfrm,
        tcocur,
        tcnump,
        tccrte,
        tcccom,
        tcddte,
        tcpnr,
        tcctry,
        tctrnr,
        tcmpas,
        tcexrt,
        tcfil1,
        tcfil2,
        fk_travel2,
        xpk_row2,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_post_travel_car') }}
),

final AS (
    SELECT
        bk_amex_travel_invoice_num,
        bk_amex_travel_invoice_dt,
        bk_amex_trvl_inv_psngr_num_int,
        bk_amx_trvl_inv_actg_brnch_num,
        bk_amex_trvl_inv_machine_id,
        bk_amex_travl_car_seq_num_int,
        currency_exchange_rate_amt,
        rental_car_rate_rprtng_usd_amt,
        rental_car_rate_local_amt,
        rental_car_company_city_name,
        rental_car_company_city_cd,
        car_booking_type_cd,
        rental_car_pickup_dt,
        rental_car_pickup_location_cd,
        rental_car_confirmation_num,
        rental_car_drop_off_dt,
        rental_car_company_zip_cd,
        rental_car_cnt,
        rental_day_cnt,
        amex_pnr_locator_id,
        rental_company_cd,
        rental_car_passenger_cnt,
        rental_car_state_cd,
        rental_car_type_cd,
        local_currency_cd,
        rental_car_cmpny_iso_cntry_cd,
        src_rptd_rental_company_cd,
        src_rptd_rental_car_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_post_travel_car
)

SELECT * FROM final