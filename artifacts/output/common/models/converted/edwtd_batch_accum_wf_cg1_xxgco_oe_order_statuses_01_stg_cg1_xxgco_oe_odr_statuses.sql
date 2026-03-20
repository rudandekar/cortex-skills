{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxgco_oe_order_statuses', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXGCO_OE_ORDER_STATUSES',
        'target_table': 'STG_CG1_XXGCO_OE_ODR_STATUSES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.767985+00:00'
    }
) }}

WITH 

source_cg1_xxgco_oe_order_statuses AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        header_id,
        line_id,
        status_result_code,
        status_result_date,
        created_program_name,
        program_id,
        request_id,
        org_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login
    FROM {{ source('raw', 'cg1_xxgco_oe_order_statuses') }}
),

source_stg_cg1_xxgco_oe_odr_statuses AS (
    SELECT
        header_id,
        line_id,
        status_result_code,
        status_result_date,
        created_program_name,
        program_id,
        request_id,
        org_id,
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
    FROM {{ source('raw', 'stg_cg1_xxgco_oe_odr_statuses') }}
),

transformed_exp_cg1_xxgco_oe_order_statuses AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    header_id,
    line_id,
    status_result_code,
    status_result_date,
    created_program_name,
    program_id,
    request_id,
    org_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login
    FROM source_stg_cg1_xxgco_oe_odr_statuses
),

final AS (
    SELECT
        header_id,
        line_id,
        status_result_code,
        status_result_date,
        created_program_name,
        program_id,
        request_id,
        org_id,
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
    FROM transformed_exp_cg1_xxgco_oe_order_statuses
)

SELECT * FROM final