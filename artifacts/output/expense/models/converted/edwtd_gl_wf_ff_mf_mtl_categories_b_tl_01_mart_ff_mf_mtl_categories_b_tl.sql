{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_mtl_categories_b_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_MTL_CATEGORIES_B_TL',
        'target_table': 'FF_MF_MTL_CATEGORIES_B_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.817678+00:00'
    }
) }}

WITH 

source_mf_mtl_categories_tl AS (
    SELECT
        category_id,
        created_by,
        creation_date,
        description,
        ges_update_date,
        global_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_lang
    FROM {{ source('raw', 'mf_mtl_categories_tl') }}
),

source_mf_mtl_categories_b AS (
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
        category_id,
        created_by,
        creation_date,
        description,
        disable_date,
        enabled_flag,
        end_date_active,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        segment1,
        segment10,
        segment11,
        segment12,
        segment13,
        segment14,
        segment15,
        segment16,
        segment17,
        segment18,
        segment19,
        segment2,
        segment20,
        segment3,
        segment4,
        segment5,
        segment6,
        segment7,
        segment8,
        segment9,
        start_date_active,
        structure_id,
        summary_flag,
        cms_replication_date,
        cms_replication_number
    FROM {{ source('raw', 'mf_mtl_categories_b') }}
),

transformed_exp_mf_mtl_categories_b_tl AS (
    SELECT
    category_id,
    global_name,
    description,
    language_code,
    ges_update_date,
    segment1,
    attribute2,
    attribute3,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_mtl_categories_b
),

final AS (
    SELECT
        batch_id,
        category_id,
        global_name,
        description,
        language_code,
        ges_update_date,
        segment1,
        attribute2,
        attribute3,
        create_datetime,
        action_code
    FROM transformed_exp_mf_mtl_categories_b_tl
)

SELECT * FROM final