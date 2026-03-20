{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cfi_cb_ctrygrp_ctry_mapping', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CFI_CB_CTRYGRP_CTRY_MAPPING',
        'target_table': 'ST_CFI_CB_CTRYGRP_CTRY_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.359792+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_ctrygrp_ctry_mapping AS (
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
    FROM {{ source('raw', 'ff_xxcfi_cb_ctrygrp_ctry_mapping') }}
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
        active_flag
    FROM source_ff_xxcfi_cb_ctrygrp_ctry_mapping
)

SELECT * FROM final