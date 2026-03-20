{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_hts_override_rate', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_HTS_OVERRIDE_RATE',
        'target_table': 'FF_XXCFI_CB_HTS_OVERRIDE_RATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.636023+00:00'
    }
) }}

WITH 

source_xxcfi_cb_hts_override_rate AS (
    SELECT
        hts_override_id,
        hts_id,
        hts_code,
        rate_override,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date
    FROM {{ source('raw', 'xxcfi_cb_hts_override_rate') }}
),

transformed_ex_ff_xxcfi_cb_hts_override_rate AS (
    SELECT
    hts_override_id,
    hts_id,
    hts_code,
    rate_override,
    start_date,
    end_date,
    created_by,
    created_date,
    modified_by,
    modified_date,
    'BatchId' AS batch_id,
    'I' AS action_cd,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_xxcfi_cb_hts_override_rate
),

final AS (
    SELECT
        batch_id,
        hts_override_id,
        hts_id,
        hts_code,
        rate_override,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_cd
    FROM transformed_ex_ff_xxcfi_cb_hts_override_rate
)

SELECT * FROM final