{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cg1_cst_cost_elements', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_CG1_CST_COST_ELEMENTS',
        'target_table': 'FF_CG1_CST_COST_ELEMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.708266+00:00'
    }
) }}

WITH 

source_cg1_cst_cost_elements AS (
    SELECT
        cost_element_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        cost_element,
        description,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_cst_cost_elements') }}
),

transformed_exp_cg1_cst_cost_elements AS (
    SELECT
    cost_element_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    cost_element,
    description,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    ges_update_date,
    global_name,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_cg1_cst_cost_elements
),

final AS (
    SELECT
        cost_element_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        cost_element,
        description,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_cg1_cst_cost_elements
)

SELECT * FROM final