{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_hts_contents', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_HTS_CONTENTS',
        'target_table': 'ST_XXCFI_CB_HTS_CONTENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.299561+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_hts_contents1 AS (
    SELECT
        batch_id,
        hts_id,
        hts_code,
        hts_desc,
        country_id,
        country_code,
        custom_hts_flag,
        mfn_duty_rate,
        sync_hts_tc_flag,
        override_flag,
        status,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        content_filter_flag,
        sync_done_flag,
        dashboard_flag,
        creation_datetime,
        action_flag
    FROM {{ source('raw', 'ff_xxcfi_cb_hts_contents1') }}
),

final AS (
    SELECT
        hts_id,
        hts_code,
        hts_desc,
        country_id,
        country_code,
        custom_hts_flag,
        mfn_duty_rate,
        sync_hts_tc_flag,
        override_flag,
        status,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        content_filter_flag,
        sync_done_flag,
        dashboard_flag,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_hts_contents1
)

SELECT * FROM final