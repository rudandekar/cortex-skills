{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxgco_exp_request_header', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXGCO_EXP_REQUEST_HEADER',
        'target_table': 'STG_CG1_XXGCO_EXP_RQST_HEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.574013+00:00'
    }
) }}

WITH 

source_stg_cg1_xxgco_exp_rqst_header AS (
    SELECT
        request_id,
        submitter,
        exp_status,
        solcat_id,
        sales_order_number,
        purchase_order_number,
        ic_order_id,
        so_header_id,
        approver,
        customer_name,
        request_type,
        structure_node_id,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        cs_team,
        theater,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxgco_exp_rqst_header') }}
),

source_cg1_xxgco_exp_request_header AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        request_id,
        submitter,
        exp_status,
        solcat_id,
        sales_order_number,
        purchase_order_number,
        ic_order_id,
        so_header_id,
        approver,
        customer_name,
        request_type,
        structure_node_id,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        cs_team,
        theater,
        comments,
        email_address6,
        email_address4,
        email_address1,
        reason_code,
        email_address3,
        email_address5,
        email_address2
    FROM {{ source('raw', 'cg1_xxgco_exp_request_header') }}
),

transformed_exp_cg1_xxgco_exp_request_header AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    request_id,
    submitter,
    exp_status,
    solcat_id,
    sales_order_number,
    purchase_order_number,
    ic_order_id,
    so_header_id,
    approver,
    customer_name,
    request_type,
    structure_node_id,
    creation_date,
    created_by,
    last_updated_date,
    last_updated_by,
    cs_team,
    theater
    FROM source_cg1_xxgco_exp_request_header
),

final AS (
    SELECT
        request_id,
        submitter,
        exp_status,
        solcat_id,
        sales_order_number,
        purchase_order_number,
        ic_order_id,
        so_header_id,
        approver,
        customer_name,
        request_type,
        structure_node_id,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        cs_team,
        theater,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxgco_exp_request_header
)

SELECT * FROM final