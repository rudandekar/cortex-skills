{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_renewal_sol_org_sol_link', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_RENEWAL_SOL_ORG_SOL_LINK',
        'target_table': 'N_RENEWAL_SOL_ORG_SOL_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.445611+00:00'
    }
) }}

WITH 

source_n_renewal_sol_org_sol_link AS (
    SELECT
        org_sol_key,
        renewal_sol_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_renewal_sol_org_sol_link') }}
),

final AS (
    SELECT
        org_sol_key,
        renewal_sol_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_renewal_sol_org_sol_link
)

SELECT * FROM final