{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_renewal_sol_org_sol_link', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_W_RENEWAL_SOL_ORG_SOL_LINK',
        'target_table': 'WI_SPLIT_CONSOLIDATED_REF_NUMBERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.015974+00:00'
    }
) }}

WITH 

source_w_renewal_sol_org_sol_link AS (
    SELECT
        org_sol_key,
        renewal_sol_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_renewal_sol_org_sol_link') }}
),

source_wi_split_concatenated_ref_numbers AS (
    SELECT
        line_id,
        line_ref_number,
        edw_create_dtm
    FROM {{ source('raw', 'wi_split_concatenated_ref_numbers') }}
),

final AS (
    SELECT
        line_id,
        line_ref_number,
        edw_create_dtm
    FROM source_wi_split_concatenated_ref_numbers
)

SELECT * FROM final