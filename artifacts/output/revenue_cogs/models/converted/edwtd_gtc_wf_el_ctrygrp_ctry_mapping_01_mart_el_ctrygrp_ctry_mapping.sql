{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ctrygrp_ctry_mapping', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CTRYGRP_CTRY_MAPPING',
        'target_table': 'EL_CTRYGRP_CTRY_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.900655+00:00'
    }
) }}

WITH 

source_st_cfi_cb_ctrygrp_ctry_mapping AS (
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
    FROM {{ source('raw', 'st_cfi_cb_ctrygrp_ctry_mapping') }}
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        active_flag
    FROM source_st_cfi_cb_ctrygrp_ctry_mapping
)

SELECT * FROM final