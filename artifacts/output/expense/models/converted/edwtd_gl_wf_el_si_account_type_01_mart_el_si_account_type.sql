{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_si_account_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_SI_ACCOUNT_TYPE',
        'target_table': 'EL_SI_ACCOUNT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.719336+00:00'
    }
) }}

WITH 

source_st_si_account_type AS (
    SELECT
        batch_id,
        account_type_id,
        account_type_desc,
        last_update_date,
        enabled_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_account_type') }}
),

final AS (
    SELECT
        account_type_id,
        account_type_desc,
        enabled_flag
    FROM source_st_si_account_type
)

SELECT * FROM final