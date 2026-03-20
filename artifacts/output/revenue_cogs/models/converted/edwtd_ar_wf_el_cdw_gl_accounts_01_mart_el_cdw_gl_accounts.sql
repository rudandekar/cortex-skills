{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cdw_gl_accounts', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_CDW_GL_ACCOUNTS',
        'target_table': 'EL_CDW_GL_ACCOUNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.569058+00:00'
    }
) }}

WITH 

source_st_cdw_gl_accounts AS (
    SELECT
        batch_id,
        gl_account_number,
        description,
        exclude_2tier_trx_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cdw_gl_accounts') }}
),

final AS (
    SELECT
        gl_account_number,
        description,
        exclude_2tier_trx_flag
    FROM source_st_cdw_gl_accounts
)

SELECT * FROM final