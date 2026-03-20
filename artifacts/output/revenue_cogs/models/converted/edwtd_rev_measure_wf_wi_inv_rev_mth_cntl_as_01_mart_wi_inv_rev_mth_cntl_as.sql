{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_inv_rev_mth_cntl_as', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_INV_REV_MTH_CNTL_AS',
        'target_table': 'WI_INV_REV_MTH_CNTL_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.138965+00:00'
    }
) }}

WITH 

source_wi_inv_rev_mth_cntl_as AS (
    SELECT
        fiscal_month_id,
        edw_create_datetime,
        edw_update_user
    FROM {{ source('raw', 'wi_inv_rev_mth_cntl_as') }}
),

final AS (
    SELECT
        fiscal_month_id,
        edw_create_datetime,
        edw_create_user
    FROM source_wi_inv_rev_mth_cntl_as
)

SELECT * FROM final