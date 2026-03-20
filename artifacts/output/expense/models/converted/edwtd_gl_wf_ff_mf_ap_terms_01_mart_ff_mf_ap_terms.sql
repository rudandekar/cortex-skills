{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_ap_terms', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_AP_TERMS',
        'target_table': 'FF_MF_AP_TERMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.100001+00:00'
    }
) }}

WITH 

source_mf_ap_terms AS (
    SELECT
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
        due_cutoff_day,
        enabled_flag,
        end_date_active,
        ges_update_date,
        global_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        rank,
        source_lang,
        start_date_active,
        term_id,
        type
    FROM {{ source('raw', 'mf_ap_terms') }}
),

final AS (
    SELECT
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
        due_cutoff_day,
        enabled_flag,
        end_date_active,
        ges_update_date,
        global_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        rank,
        source_lang,
        start_date_active,
        term_id,
        type
    FROM source_mf_ap_terms
)

SELECT * FROM final