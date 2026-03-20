{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_override_country_list', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_OVERRIDE_COUNTRY_LIST',
        'target_table': 'ST_XXCFI_CB_OVER_CNTRY_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.810188+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_over_cntry_list AS (
    SELECT
        batch_id,
        override_id,
        ship_to_country,
        override_country_group_id,
        override_country_group_code,
        override_country_code,
        comments,
        active_flag,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_over_cntry_list') }}
),

final AS (
    SELECT
        batch_id,
        override_id,
        ship_to_country,
        override_country_group_id,
        override_country_group_code,
        override_country_code,
        comments,
        active_flag,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_over_cntry_list
)

SELECT * FROM final