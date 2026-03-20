{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_countries', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_COUNTRIES',
        'target_table': 'FF_XXCFI_CB_COUNTRIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.082827+00:00'
    }
) }}

WITH 

source_xxcfi_cb_countries AS (
    SELECT
        country_id,
        country_code,
        country_name,
        description,
        created_by,
        created_date,
        modified_by,
        modified_date,
        interface_items
    FROM {{ source('raw', 'xxcfi_cb_countries') }}
),

transformed_ex_ff_xxcfi_cb_countries AS (
    SELECT
    country_id,
    country_code,
    country_name,
    description,
    created_by,
    created_date,
    modified_by,
    modified_date,
    interface_items,
    'BatchId' AS batch_id,
    'I' AS action_cd,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_xxcfi_cb_countries
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
        action_cd
    FROM transformed_ex_ff_xxcfi_cb_countries
)

SELECT * FROM final