{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_olm_process_dt_cntrl', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_OLM_PROCESS_DT_CNTRL',
        'target_table': 'OLM_PROCESS_DT_CNTRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.460916+00:00'
    }
) }}

WITH 

source_olm_process_dt_cntrl AS (
    SELECT
        fiscal_year_number_int,
        fiscal_year_mth_number_int,
        fiscal_month_number_int,
        process_date,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'olm_process_dt_cntrl') }}
),

filtered_fl_olm_process_dt_cntrl AS (
    SELECT *
    FROM source_olm_process_dt_cntrl
    WHERE FALSE
),

final AS (
    SELECT
        fiscal_year_number_int,
        fiscal_year_mth_number_int,
        fiscal_month_number_int,
        process_date,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM filtered_fl_olm_process_dt_cntrl
)

SELECT * FROM final