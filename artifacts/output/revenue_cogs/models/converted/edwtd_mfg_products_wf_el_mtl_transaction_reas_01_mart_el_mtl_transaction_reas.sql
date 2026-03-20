{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_mtl_transaction_reas', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_MTL_TRANSACTION_REAS',
        'target_table': 'EL_MTL_TRANSACTION_REAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.507609+00:00'
    }
) }}

WITH 

source_st_cg1_mtl_transaction_reas AS (
    SELECT
        reason_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        reason_name,
        last_update_login,
        description,
        disable_date,
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
        attribute_category,
        workflow_name,
        workflow_display_name,
        workflow_process,
        workflow_display_process,
        reason_type,
        reason_type_display,
        reason_context_code,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_mtl_transaction_reas') }}
),

final AS (
    SELECT
        reason_id,
        reason_name,
        global_name
    FROM source_st_cg1_mtl_transaction_reas
)

SELECT * FROM final