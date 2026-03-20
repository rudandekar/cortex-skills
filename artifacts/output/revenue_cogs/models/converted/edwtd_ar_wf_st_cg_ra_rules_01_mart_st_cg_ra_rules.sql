{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg_ra_rules', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CG_RA_RULES',
        'target_table': 'ST_CG_RA_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.587573+00:00'
    }
) }}

WITH 

source_st_cg_ra_rules AS (
    SELECT
        batch_id,
        rule_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        name,
        type_code,
        status,
        frequency,
        occurrences,
        description,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        deferred_revenue_flag,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg_ra_rules') }}
),

final AS (
    SELECT
        batch_id,
        rule_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        name,
        type_code,
        status,
        frequency,
        occurrences,
        description,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        deferred_revenue_flag,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM source_st_cg_ra_rules
)

SELECT * FROM final