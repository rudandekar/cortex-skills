{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fnd_lookup_values_cfi_adj_type', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_FND_LOOKUP_VALUES_CFI_ADJ_TYPE',
        'target_table': 'EL_FND_LOOKUP_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.488590+00:00'
    }
) }}

WITH 

source_st_cg_fnd_lookup_values_ar AS (
    SELECT
        batch_id,
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
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg_fnd_lookup_values_ar') }}
),

final AS (
    SELECT
        lookup_type,
        lang,
        security_group_id,
        lookup_code,
        meaning,
        view_application_id,
        global_name,
        enabled_flag,
        description,
        create_datetime,
        ges_update_date,
        start_date_active,
        end_date_active,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5
    FROM source_st_cg_fnd_lookup_values_ar
)

SELECT * FROM final