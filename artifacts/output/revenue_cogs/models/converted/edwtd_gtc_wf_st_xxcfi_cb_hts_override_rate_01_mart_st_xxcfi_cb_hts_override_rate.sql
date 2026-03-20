{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_hts_override_rate', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_HTS_OVERRIDE_RATE',
        'target_table': 'ST_XXCFI_CB_HTS_OVERRIDE_RATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.311739+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_hts_override_rate AS (
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
    FROM {{ source('raw', 'ff_xxcfi_cb_hts_override_rate') }}
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
        action_code
    FROM source_ff_xxcfi_cb_hts_override_rate
)

SELECT * FROM final