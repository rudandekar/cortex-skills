{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_account_local', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_ACCOUNT_LOCAL',
        'target_table': 'ST_SI_ACCOUNT_LOCAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.878172+00:00'
    }
) }}

WITH 

source_ff_si_account_local AS (
    SELECT
        batch_id,
        account_value,
        si_flex_struct_id,
        enabled_flag,
        reclass_account_value,
        end_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_account_local') }}
),

final AS (
    SELECT
        batch_id,
        account_value,
        si_flex_struct_id,
        enabled_flag,
        reclass_account_value,
        end_date,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_account_local
)

SELECT * FROM final