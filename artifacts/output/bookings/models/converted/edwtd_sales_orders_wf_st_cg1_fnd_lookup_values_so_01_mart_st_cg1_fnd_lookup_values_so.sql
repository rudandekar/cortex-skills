{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_fnd_lookup_values_so', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_FND_LOOKUP_VALUES_SO',
        'target_table': 'ST_CG1_FND_LOOKUP_VALUES_SO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.035655+00:00'
    }
) }}

WITH 

source_cg1_fnd_lookup_values AS (
    SELECT
        lookup_type,
        lang,
        lookup_code,
        meaning,
        description,
        enabled_flag,
        start_date_active,
        end_date_active,
        created_by,
        creation_date,
        last_updated_by,
        last_update_login,
        last_update_date,
        source_lang,
        security_group_id,
        view_application_id,
        territory_code,
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
        tag,
        leaf_node,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_fnd_lookup_values') }}
),

final AS (
    SELECT
        lookup_type,
        lang,
        lookup_code,
        meaning,
        description,
        enabled_flag,
        start_date_active,
        end_date_active,
        created_by,
        creation_date,
        last_updated_by,
        last_update_login,
        last_update_date,
        source_lang,
        security_group_id,
        view_application_id,
        territory_code,
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
        tag,
        leaf_node,
        source_commit_time,
        global_name
    FROM source_cg1_fnd_lookup_values
)

SELECT * FROM final