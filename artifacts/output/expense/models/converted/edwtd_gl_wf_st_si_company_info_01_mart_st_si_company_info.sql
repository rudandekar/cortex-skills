{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_company_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_COMPANY_INFO',
        'target_table': 'ST_SI_COMPANY_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.963728+00:00'
    }
) }}

WITH 

source_ff_si_company_info AS (
    SELECT
        batch_id,
        company_value,
        theater_id,
        company_name,
        functional_currency_code,
        enabled_flag,
        usage_description,
        start_date,
        end_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_company_info') }}
),

final AS (
    SELECT
        batch_id,
        company_value,
        theater_id,
        company_name,
        functional_currency_code,
        enabled_flag,
        usage_description,
        start_date,
        end_date,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_company_info
)

SELECT * FROM final