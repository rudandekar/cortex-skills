{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg_xxcfir_weee_fee_lines', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CG_XXCFIR_WEEE_FEE_LINES',
        'target_table': 'ST_CG_XXCFIR_WEEE_FEE_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.356121+00:00'
    }
) }}

WITH 

source_cg1_xxcfir_weee_fee_lines AS (
    SELECT
        weee_fee_header_id,
        weee_fee_line_id,
        inventory_item_id,
        product_name,
        unit_of_measure,
        application_method,
        operand,
        start_date_active,
        end_date_active,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
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
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_xxcfir_weee_fee_lines') }}
),

transformed_exptrans AS (
    SELECT
    weee_fee_header_id,
    weee_fee_line_id,
    inventory_item_id,
    product_name,
    unit_of_measure,
    application_method,
    operand,
    start_date_active,
    end_date_active,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
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
    source_commit_time,
    global_name,
    create_datetime,
    action_code,
    1 AS batch_id
    FROM source_cg1_xxcfir_weee_fee_lines
),

final AS (
    SELECT
        batch_id,
        weee_fee_header_id,
        weee_fee_line_id,
        inventory_item_id,
        product_name,
        unit_of_measure,
        application_method,
        operand,
        start_date_active,
        end_date_active,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
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
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM transformed_exptrans
)

SELECT * FROM final