{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_company_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_COMPANY_INFO',
        'target_table': 'FF_SI_COMPANY_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.899031+00:00'
    }
) }}

WITH 

source_si_company_info AS (
    SELECT
        company_value,
        theater_id,
        company_name,
        functional_currency_code,
        enabled_flag,
        usage_description,
        start_date,
        end_date,
        created_by,
        create_date,
        last_updated_by,
        last_update_date,
        available_for_dept,
        info_published,
        erp_published
    FROM {{ source('raw', 'si_company_info') }}
),

transformed_exp_si_company_info AS (
    SELECT
    company_value,
    theater_id,
    company_name,
    functional_currency_code,
    enabled_flag,
    usage_description,
    start_date,
    end_date,
    last_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_company_info
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
    FROM transformed_exp_si_company_info
)

SELECT * FROM final