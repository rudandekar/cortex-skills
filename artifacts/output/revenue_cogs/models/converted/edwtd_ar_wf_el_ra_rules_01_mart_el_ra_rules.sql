{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ra_rules', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_RA_RULES',
        'target_table': 'EL_RA_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.802753+00:00'
    }
) }}

WITH 

source_st_om_ra_rules AS (
    SELECT
        batch_id,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category,
        created_by,
        creation_date,
        deferred_revenue_flag,
        description,
        frequency,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        occurrences,
        rule_id,
        status,
        type_code,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_ra_rules') }}
),

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
        rule_id,
        global_name,
        name,
        description,
        type_code,
        status,
        creation_date,
        last_update_date,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_st_cg_ra_rules
)

SELECT * FROM final