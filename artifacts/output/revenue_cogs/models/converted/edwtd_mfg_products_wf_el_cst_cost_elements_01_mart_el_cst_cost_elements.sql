{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cst_cost_elements', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_CST_COST_ELEMENTS',
        'target_table': 'EL_CST_COST_ELEMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.329887+00:00'
    }
) }}

WITH 

source_st_cg1_cst_cost_elements AS (
    SELECT
        cost_element_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        cost_element,
        description,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        global_name,
        ges_update_date,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_cst_cost_elements') }}
),

final AS (
    SELECT
        cost_element_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        cost_element,
        description,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        global_name,
        ges_update_date,
        batch_id,
        create_datetime,
        action_code
    FROM source_st_cg1_cst_cost_elements
)

SELECT * FROM final