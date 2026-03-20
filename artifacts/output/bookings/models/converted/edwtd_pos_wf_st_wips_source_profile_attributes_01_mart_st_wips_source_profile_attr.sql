{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_source_profile_attributes', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_SOURCE_PROFILE_ATTRIBUTES',
        'target_table': 'ST_WIPS_SOURCE_PROFILE_ATTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.343869+00:00'
    }
) }}

WITH 

source_ff_wips_source_profile_attr AS (
    SELECT
        attribute1,
        source_profile_id,
        attribute_value,
        active_flag,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        source_name,
        territory_id,
        sales_territory_id,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        create_datetime,
        batch_id,
        action_code
    FROM {{ source('raw', 'ff_wips_source_profile_attr') }}
),

final AS (
    SELECT
        attribute1,
        source_profile_id,
        attribute_value,
        active_flag,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        source_name,
        territory_id,
        sales_territory_id,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        create_datetime,
        batch_id,
        action_code
    FROM source_ff_wips_source_profile_attr
)

SELECT * FROM final