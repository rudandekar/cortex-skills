{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_unbd_rstd_rpo_mth_ctrl', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_AS_UNBD_RSTD_RPO_MTH_CTRL',
        'target_table': 'WI_AS_UNBD_RSTD_RPO_MTH_CTRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.286885+00:00'
    }
) }}

WITH 

source_wi_as_unbd_rstd_rpo_mth_ctrl AS (
    SELECT
        fiscal_year_mth_int,
        edw_create_dtm
    FROM {{ source('raw', 'wi_as_unbd_rstd_rpo_mth_ctrl') }}
),

final AS (
    SELECT
        fiscal_year_mth_int,
        edw_create_dtm
    FROM source_wi_as_unbd_rstd_rpo_mth_ctrl
)

SELECT * FROM final