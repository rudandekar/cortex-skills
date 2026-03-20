{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cdw_gl_accounts', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_FF_CDW_GL_ACCOUNTS',
        'target_table': 'FF_CDW_GL_ACCOUNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.709678+00:00'
    }
) }}

WITH 

source_cdw_gl_accounts AS (
    SELECT
        gl_account_number,
        description,
        include_chatt_flag,
        exclude_2tier_trx_flag
    FROM {{ source('raw', 'cdw_gl_accounts') }}
),

transformed_exp_cdw_gl_accounts AS (
    SELECT
    gl_account_number,
    description,
    exclude_2tier_trx_flag,
    'BatchId' AS batch_id,
    'I' AS action_code,
    TO_CHAR(GL_ACCOUNT_NUMBER) AS o_gl_account_number,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_cdw_gl_accounts
),

final AS (
    SELECT
        batch_id,
        gl_account_number,
        description,
        exclude_2tier_trx_flag,
        create_datetime,
        action_code
    FROM transformed_exp_cdw_gl_accounts
)

SELECT * FROM final