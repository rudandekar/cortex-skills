{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg_xxcfir_sol_inv_hold_al', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CG_XXCFIR_SOL_INV_HOLD_AL',
        'target_table': 'ST_CG_XXCFIR_SOL_INV_HOLD_AL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.842247+00:00'
    }
) }}

WITH 

source_cg1_xxcfir_sol_inv_hold_all AS (
    SELECT
        db_source_name,
        order_number,
        so_line_id,
        inv_hold,
        org_id,
        trx_number,
        trx_date,
        interface_line_attribute1,
        interface_line_attribute2,
        interface_line_attribute3,
        interface_line_attribute4,
        interface_line_attribute5,
        interface_line_attribute6,
        interface_line_attribute7,
        interface_line_attribute8,
        interface_line_attribute9,
        interface_line_attribute10,
        interface_line_attribute11,
        interface_line_attribute12,
        interface_line_attribute13,
        interface_line_attribute14,
        interface_line_attribute15,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_xxcfir_sol_inv_hold_all') }}
),

final AS (
    SELECT
        batch_id,
        db_source_name,
        order_number,
        so_line_id,
        inv_hold,
        org_id,
        trx_number,
        trx_date,
        interface_line_attribute1,
        interface_line_attribute2,
        interface_line_attribute3,
        interface_line_attribute4,
        interface_line_attribute5,
        interface_line_attribute6,
        interface_line_attribute7,
        interface_line_attribute8,
        interface_line_attribute9,
        interface_line_attribute10,
        interface_line_attribute11,
        interface_line_attribute12,
        interface_line_attribute13,
        interface_line_attribute14,
        interface_line_attribute15,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM source_cg1_xxcfir_sol_inv_hold_all
)

SELECT * FROM final