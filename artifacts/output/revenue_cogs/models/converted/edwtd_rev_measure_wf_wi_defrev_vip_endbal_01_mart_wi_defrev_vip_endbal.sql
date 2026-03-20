{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_vip_endbal', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_VIP_ENDBAL',
        'target_table': 'WI_DEFREV_VIP_ENDBAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.689087+00:00'
    }
) }}

WITH 

source_wi_defrev_vip_endbal AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_incentive_program_name,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        trans_fiscal_year_month_int,
        booking_accrual_usd_amt
    FROM {{ source('raw', 'wi_defrev_vip_endbal') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_incentive_program_name,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        trans_fiscal_year_month_int,
        booking_accrual_usd_amt
    FROM source_wi_defrev_vip_endbal
)

SELECT * FROM final