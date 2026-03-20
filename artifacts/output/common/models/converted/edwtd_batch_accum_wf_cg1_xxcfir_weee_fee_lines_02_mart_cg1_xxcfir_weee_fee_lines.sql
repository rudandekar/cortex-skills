{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxcfir_weee_fee_lines', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXCFIR_WEEE_FEE_LINES',
        'target_table': 'CG1_XXCFIR_WEEE_FEE_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.745501+00:00'
    }
) }}

WITH 

source_stg_cg1_xxcfir_weee_fee_lines AS (
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
    FROM {{ source('raw', 'stg_cg1_xxcfir_weee_fee_lines') }}
),

source_cg1_xxcfir_weee_fee_lines AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
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
        account,
        subaccount,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10
    FROM {{ source('raw', 'cg1_xxcfir_weee_fee_lines') }}
),

transformed_exp_cg1_xxcfir_weee_fee_lines AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
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
    attribute10
    FROM source_cg1_xxcfir_weee_fee_lines
),

final AS (
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
    FROM transformed_exp_cg1_xxcfir_weee_fee_lines
)

SELECT * FROM final