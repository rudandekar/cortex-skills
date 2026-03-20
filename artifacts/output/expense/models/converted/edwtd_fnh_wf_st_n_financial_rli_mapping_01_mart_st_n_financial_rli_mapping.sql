{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_n_financial_rli_mapping', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_ST_N_FINANCIAL_RLI_MAPPING',
        'target_table': 'ST_N_FINANCIAL_RLI_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.901680+00:00'
    }
) }}

WITH 

source_ff_n_financial_rli_mapping AS (
    SELECT
        batch_id,
        rli_member,
        rli_member_desc,
        entity_low,
        entity_high,
        location_low,
        location_high,
        department_low,
        department_high,
        account_low,
        account_high,
        refresh_date,
        create_datetime,
        action_code,
        inclusion_flag
    FROM {{ source('raw', 'ff_n_financial_rli_mapping') }}
),

final AS (
    SELECT
        batch_id,
        rli_member,
        rli_member_desc,
        entity_low,
        entity_high,
        location_low,
        location_high,
        department_low,
        department_high,
        account_low,
        account_high,
        refresh_date,
        create_datetime,
        action_code,
        inclusion_flag
    FROM source_ff_n_financial_rli_mapping
)

SELECT * FROM final