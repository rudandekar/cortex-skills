{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcca_weee_fee_lines_cg', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCCA_WEEE_FEE_LINES_CG',
        'target_table': 'EL_XXCCA_WEEE_FEE_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.584734+00:00'
    }
) }}

WITH 

source_st_cg_xxcfir_weee_fee_lines AS (
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
    FROM {{ source('raw', 'st_cg_xxcfir_weee_fee_lines') }}
),

final AS (
    SELECT
        weee_fee_header_id,
        weee_fee_line_id,
        global_name,
        inventory_item_id,
        product_name,
        unit_of_measure,
        application_method,
        start_date_active,
        end_date_active,
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
        operand
    FROM source_st_cg_xxcfir_weee_fee_lines
)

SELECT * FROM final