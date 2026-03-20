{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_rev_mth_ctrl_stlt_ye', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_REV_MTH_CTRL_STLT_YE',
        'target_table': 'WI_RSTD_REV_MTH_CTRL_STLT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.129505+00:00'
    }
) }}

WITH 

source_wi_rstd_rev_mth_ctrl_stlt AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        active_ind,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        fiscal_year_number_int,
        fiscal_quarter_number_int,
        job_stream_id
    FROM {{ source('raw', 'wi_rstd_rev_mth_ctrl_stlt') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        active_ind,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        fiscal_year_number_int,
        fiscal_quarter_number_int,
        job_stream_id
    FROM source_wi_rstd_rev_mth_ctrl_stlt
)

SELECT * FROM final