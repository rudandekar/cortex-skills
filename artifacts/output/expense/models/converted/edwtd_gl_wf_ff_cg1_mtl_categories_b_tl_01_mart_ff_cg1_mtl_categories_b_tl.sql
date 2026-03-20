{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cg1_mtl_categories_b_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_CG1_MTL_CATEGORIES_B_TL',
        'target_table': 'FF_CG1_MTL_CATEGORIES_B_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.935045+00:00'
    }
) }}

WITH 

source_cg1_mtl_categories_tl AS (
    SELECT
        category_id,
        language,
        source_lang,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        description,
        last_update_login,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_mtl_categories_tl') }}
),

source_cg1_mtl_categories_b AS (
    SELECT
        category_id,
        structure_id,
        summary_flag,
        enabled_flag,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        description,
        disable_date,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        segment7,
        segment8,
        segment9,
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
        segment20,
        start_date_active,
        end_date_active,
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
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        web_status,
        supplier_enabled_flag,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_mtl_categories_b') }}
),

transformed_exp_cg1_mtl_categories_b_tl AS (
    SELECT
    category_id,
    language,
    description,
    segment1,
    ges_update_date,
    global_name,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_created_datetime,
    'I' AS o_action_code
    FROM source_cg1_mtl_categories_b
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
        create_datetime,
        action_code
    FROM transformed_exp_cg1_mtl_categories_b_tl
)

SELECT * FROM final