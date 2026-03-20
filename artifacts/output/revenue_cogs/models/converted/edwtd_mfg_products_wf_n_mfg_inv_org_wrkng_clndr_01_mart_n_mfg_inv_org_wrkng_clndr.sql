{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_mfg_inv_org_wrkng_clndr', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_MFG_INV_ORG_WRKNG_CLNDR',
        'target_table': 'N_MFG_INV_ORG_WRKNG_CLNDR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.214835+00:00'
    }
) }}

WITH 

source_w_mfg_inv_org_wrkng_clndr AS (
    SELECT
        inv_org_name_key,
        bk_calendar_dt,
        calendar_date_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_mfg_inv_org_wrkng_clndr') }}
),

final AS (
    SELECT
        inv_org_name_key,
        bk_calendar_dt,
        calendar_date_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_mfg_inv_org_wrkng_clndr
)

SELECT * FROM final