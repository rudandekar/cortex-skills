{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ar_adjustment_status', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_AR_ADJUSTMENT_STATUS',
        'target_table': 'W_AR_ADJUSTMENT_STATUS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.353451+00:00'
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

source_st_om_fnd_lookup_values_ar AS (
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
        description,
        enabled_flag,
        end_date_active,
        ges_update_date,
        global_name,
        lang,
        last_updated_by,
        last_update_date,
        last_update_login,
        lookup_code,
        lookup_type,
        meaning,
        security_group_id,
        source_lang,
        start_date_active,
        tag,
        territory_code,
        view_application_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_fnd_lookup_values_ar') }}
),

final AS (
    SELECT
        bk_ar_adjustment_status_cd,
        ar_adjustment_status_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_om_fnd_lookup_values_ar
)

SELECT * FROM final