{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fin_account_ldsg', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_FIN_ACCOUNT_LDSG',
        'target_table': 'ST_FIN_ACCOUNT_LDSG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.747067+00:00'
    }
) }}

WITH 

source_ldsg_custom_account_hierarchy AS (
    SELECT
        account_num,
        spend_roll_up,
        spend_category,
        spend_group
    FROM {{ source('raw', 'ldsg_custom_account_hierarchy') }}
),

transformed_exptrans AS (
    SELECT
    account_num,
    spend_roll_up,
    spend_category,
    spend_group
    FROM source_ldsg_custom_account_hierarchy
),

final AS (
    SELECT
        account_num,
        spend_roll_up,
        spend_category,
        spend_group
    FROM transformed_exptrans
)

SELECT * FROM final