{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_ctrygrp_ctry_mapping', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_CTRYGRP_CTRY_MAPPING',
        'target_table': 'FF_XXCFI_CB_CTRYGRP_CTRY_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.829722+00:00'
    }
) }}

WITH 

source_xxcfi_cb_ctrygrp_ctry_mapping AS (
    SELECT
        country_group_mapping_id,
        country_group_id,
        country_group_code,
        country_id,
        country_code,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        active_flag
    FROM {{ source('raw', 'xxcfi_cb_ctrygrp_ctry_mapping') }}
),

transformed_ex_ff_xxcfi_cb_ctrygrp_ctry_mapping AS (
    SELECT
    country_group_mapping_id,
    country_group_id,
    country_group_code,
    country_id,
    country_code,
    start_date,
    end_date,
    created_by,
    created_date,
    modified_by,
    modified_date,
    active_flag,
    'BatchId' AS batch_id,
    'I' AS action_cd,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_xxcfi_cb_ctrygrp_ctry_mapping
),

final AS (
    SELECT
        country_group_mapping_id,
        country_group_id,
        country_group_code,
        country_id,
        country_code,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        active_flag,
        batch_id,
        action_cd,
        create_datetime
    FROM transformed_ex_ff_xxcfi_cb_ctrygrp_ctry_mapping
)

SELECT * FROM final