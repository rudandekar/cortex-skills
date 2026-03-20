{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_account_local', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_ACCOUNT_LOCAL',
        'target_table': 'FF_SI_ACCOUNT_LOCAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.780473+00:00'
    }
) }}

WITH 

source_si_account_local AS (
    SELECT
        account_value,
        si_flex_struct_id,
        enabled_flag,
        reclass_account_value,
        end_date,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        info_published,
        erp_published
    FROM {{ source('raw', 'si_account_local') }}
),

transformed_exp_set_default_values AS (
    SELECT
    account_value,
    last_update_date,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_si_account_local
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
    FROM transformed_exp_set_default_values
)

SELECT * FROM final