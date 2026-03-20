{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_distributor_claim_group', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTRIBUTOR_CLAIM_GROUP',
        'target_table': 'EX_DCA_HEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.407300+00:00'
    }
) }}

WITH 

source_ex_dca_header AS (
    SELECT
        dca_id,
        oracle_reason_code,
        salesrep_name,
        comments,
        justification,
        dca_status,
        dca_approved_by,
        proxy_approver,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        saf_interface_date,
        group_id,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_dca_header') }}
),

source_st_dca_header AS (
    SELECT
        dca_id,
        oracle_reason_code,
        salesrep_name,
        comments,
        justification,
        dca_status,
        dca_approved_by,
        proxy_approver,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        saf_interface_date,
        group_id,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_dca_header') }}
),

final AS (
    SELECT
        dca_id,
        oracle_reason_code,
        salesrep_name,
        comments,
        justification,
        dca_status,
        dca_approved_by,
        proxy_approver,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        saf_interface_date,
        group_id,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM source_st_dca_header
)

SELECT * FROM final