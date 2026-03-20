{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_def_rev_mth_ctrl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DEF_REV_MTH_CTRL',
        'target_table': 'WI_RSDT_DEF_REV_MTH_CTRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.405563+00:00'
    }
) }}

WITH 

source_wi_rsdt_def_rev_mth_ctrl AS (
    SELECT
        fiscal_year_month_int,
        active_ind,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'wi_rsdt_def_rev_mth_ctrl') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        active_ind,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_wi_rsdt_def_rev_mth_ctrl
)

SELECT * FROM final