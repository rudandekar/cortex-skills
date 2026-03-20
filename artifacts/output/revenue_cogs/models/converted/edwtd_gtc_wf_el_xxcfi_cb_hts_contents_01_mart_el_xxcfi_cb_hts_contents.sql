{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfi_cb_hts_contents', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFI_CB_HTS_CONTENTS',
        'target_table': 'EL_XXCFI_CB_HTS_CONTENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.789769+00:00'
    }
) }}

WITH 

source_st_xxcfi_cb_hts_contents AS (
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
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcfi_cb_hts_contents') }}
),

final AS (
    SELECT
        hts_id,
        hts_code,
        start_date,
        country_code,
        status,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_st_xxcfi_cb_hts_contents
)

SELECT * FROM final