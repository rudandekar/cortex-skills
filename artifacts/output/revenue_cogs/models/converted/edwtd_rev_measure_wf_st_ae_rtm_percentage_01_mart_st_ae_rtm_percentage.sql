{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_rtm_percentage', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_AE_RTM_PERCENTAGE',
        'target_table': 'ST_AE_RTM_PERCENTAGE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.840324+00:00'
    }
) }}

WITH 

source_ae_rtm_percentage AS (
    SELECT
        fiscal_month_id,
        sub_measure_key,
        driver_type,
        rtm_value,
        rtm_percentage,
        create_date,
        created_by,
        update_date,
        updated_by
    FROM {{ source('raw', 'ae_rtm_percentage') }}
),

final AS (
    SELECT
        fiscal_month_id,
        sub_measure_key,
        driver_type,
        rtm_value,
        rtm_percentage,
        create_date,
        created_by,
        update_date,
        updated_by
    FROM source_ae_rtm_percentage
)

SELECT * FROM final