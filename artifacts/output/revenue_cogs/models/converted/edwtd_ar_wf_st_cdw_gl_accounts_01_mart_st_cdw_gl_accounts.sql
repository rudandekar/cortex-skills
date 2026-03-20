{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cdw_gl_accounts', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CDW_GL_ACCOUNTS',
        'target_table': 'ST_CDW_GL_ACCOUNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.662125+00:00'
    }
) }}

WITH 

source_ff_cdw_gl_accounts AS (
    SELECT
        batch_id,
        gl_account_number,
        description,
        exclude_2tier_trx_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_cdw_gl_accounts') }}
),

final AS (
    SELECT
        batch_id,
        gl_account_number,
        description,
        exclude_2tier_trx_flag,
        create_datetime,
        action_code
    FROM source_ff_cdw_gl_accounts
)

SELECT * FROM final