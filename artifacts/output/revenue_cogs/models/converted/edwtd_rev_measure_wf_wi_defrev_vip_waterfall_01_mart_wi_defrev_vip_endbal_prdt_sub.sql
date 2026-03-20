{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_vip_waterfall', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_VIP_WATERFALL',
        'target_table': 'WI_DEFREV_VIP_ENDBAL_PRDT_SUB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.111724+00:00'
    }
) }}

WITH 

source_wi_defrev_vip_endbal_prdt_sub AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_incentive_program_name,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        trans_fiscal_year_month_int,
        booking_accrual_usd_amt
    FROM {{ source('raw', 'wi_defrev_vip_endbal_prdt_sub') }}
),

source_wi_defrev_vip_recur_src AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_incentive_program_name,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        trans_fiscal_year_month_int,
        booking_accrual_usd_amt
    FROM {{ source('raw', 'wi_defrev_vip_recur_src') }}
),

source_wi_defrev_vip_product_key AS (
    SELECT
        product_key
    FROM {{ source('raw', 'wi_defrev_vip_product_key') }}
),

source_el_defrev_vip_bal AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_incentive_program_name,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        trans_fiscal_year_month_int,
        booking_accrual_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_defrev_vip_bal') }}
),

source_wi_defrev_vip_waterfall AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_incentive_program_name,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        trans_fiscal_year_month_int,
        defrev_vip_wtrfal_amt,
        defrev_vip_projected_bal_amt,
        defrev_vip_wtrfal_period,
        wtrfl_fiscal_year_month_int,
        month_age,
        depth
    FROM {{ source('raw', 'wi_defrev_vip_waterfall') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_incentive_program_name,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        trans_fiscal_year_month_int,
        booking_accrual_usd_amt
    FROM source_wi_defrev_vip_waterfall
)

SELECT * FROM final