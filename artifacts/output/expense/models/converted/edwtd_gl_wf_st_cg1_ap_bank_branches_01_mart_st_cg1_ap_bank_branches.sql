{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_ap_bank_branches', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_AP_BANK_BRANCHES',
        'target_table': 'ST_CG1_AP_BANK_BRANCHES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.833712+00:00'
    }
) }}

WITH 

source_ff_cg1_ap_bank_branches AS (
    SELECT
        batch_id,
        bank_branch_id,
        last_update_date,
        last_updated_by,
        bank_name,
        bank_branch_name,
        bank_num,
        creation_date,
        created_by,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_cg1_ap_bank_branches') }}
),

final AS (
    SELECT
        batch_id,
        bank_branch_id,
        last_update_date,
        last_updated_by,
        bank_name,
        bank_branch_name,
        bank_num,
        creation_date,
        created_by,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM source_ff_cg1_ap_bank_branches
)

SELECT * FROM final