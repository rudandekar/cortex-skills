{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcca_weee_fee_lines', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCCA_WEEE_FEE_LINES',
        'target_table': 'EL_XXCCA_WEEE_FEE_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.211925+00:00'
    }
) }}

WITH 

source_st_om_xxcca_weee_fee_lines AS (
    SELECT
        batch_id,
        weee_fee_header_id,
        weee_fee_line_id,
        global_name,
        inventory_item_id,
        product_name,
        unit_of_measure,
        application_method,
        operand,
        start_date_active,
        end_date_active,
        created_by,
        last_update_by,
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
        create_datetime,
        action_code,
        ges_update_date,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'st_om_xxcca_weee_fee_lines') }}
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
    FROM source_st_om_xxcca_weee_fee_lines
)

SELECT * FROM final