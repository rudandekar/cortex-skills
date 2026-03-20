{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dca_header', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_DCA_HEADER',
        'target_table': 'ST_DCA_HEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.403040+00:00'
    }
) }}

WITH 

source_ff_dca_header AS (
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
    FROM {{ source('raw', 'ff_dca_header') }}
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
        action_code
    FROM source_ff_dca_header
)

SELECT * FROM final