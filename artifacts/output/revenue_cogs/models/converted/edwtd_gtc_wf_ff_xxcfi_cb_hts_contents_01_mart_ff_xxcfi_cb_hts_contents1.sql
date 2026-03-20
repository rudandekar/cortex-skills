{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_hts_contents', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_HTS_CONTENTS',
        'target_table': 'FF_XXCFI_CB_HTS_CONTENTS1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.006550+00:00'
    }
) }}

WITH 

source_xxcfi_cb_hts_contents AS (
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
        dashboard_flag
    FROM {{ source('raw', 'xxcfi_cb_hts_contents') }}
),

transformed_exp_ff_xxcfi_cb_hts_contents AS (
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
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_hts_contents
),

final AS (
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
        create_datetime,
        action_code
    FROM transformed_exp_ff_xxcfi_cb_hts_contents
)

SELECT * FROM final