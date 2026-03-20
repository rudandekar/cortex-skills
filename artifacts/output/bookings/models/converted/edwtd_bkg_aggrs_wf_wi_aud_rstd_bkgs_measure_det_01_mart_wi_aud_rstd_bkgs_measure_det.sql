{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_aud_rstd_bkgs_measure_det', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_AUD_RSTD_BKGS_MEASURE_DET',
        'target_table': 'WI_AUD_RSTD_BKGS_MEASURE_DET',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.794056+00:00'
    }
) }}

WITH 

source_wi_aud_rstd_bkgs_measure_det AS (
    SELECT
        audit_table_name,
        bookings_measure_key,
        dv_fiscal_year_mth_number_int,
        bkgs_measure_trans_type_code,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'wi_aud_rstd_bkgs_measure_det') }}
),

transformed_exptrans AS (
    SELECT
    audit_table_name,
    audit_column_name,
    missing_keys_count,
    IFF(MISSING_KEYS_COUNT >= 1 , ABORT('THERE ARE MISSING MEASURE KEYS FOR THE CURRENT FISCAL MONTH' ) ) AS missing_keys_count_out
    FROM source_wi_aud_rstd_bkgs_measure_det
),

filtered_filtrans AS (
    SELECT *
    FROM transformed_exptrans
    WHERE FALSE
),

final AS (
    SELECT
        audit_table_name,
        bookings_measure_key,
        dv_fiscal_year_mth_number_int,
        bkgs_measure_trans_type_code,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM filtered_filtrans
)

SELECT * FROM final