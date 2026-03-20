{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_countries', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_COUNTRIES',
        'target_table': 'ST_XXCFI_CB_COUNTRIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.725992+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_countries AS (
    SELECT
        batch_id,
        country_id,
        country_code,
        country_name,
        description,
        created_by,
        created_date,
        modified_by,
        modified_date,
        interface_items,
        create_datetime,
        action_cd
    FROM {{ source('raw', 'ff_xxcfi_cb_countries') }}
),

final AS (
    SELECT
        batch_id,
        country_id,
        country_code,
        country_name,
        description,
        created_by,
        created_date,
        modified_by,
        modified_date,
        interface_items,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_countries
)

SELECT * FROM final