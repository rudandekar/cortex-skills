{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcmf_sp_bts_extr_pid', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_XXCMF_SP_BTS_EXTR_PID',
        'target_table': 'FF_XXCMF_SP_BTS_EXTR_PID',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.422297+00:00'
    }
) }}

WITH 

source_cg1_xxcmf_sp_bts_extract_pid AS (
    SELECT
        pid,
        pid_organization_code,
        on_hand,
        unit_cost,
        eos_date,
        orderability,
        publish_date,
        creation_date,
        created_by,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_xxcmf_sp_bts_extract_pid') }}
),

transformed_exp_xxcmf_sp_bts_extract_pid AS (
    SELECT
    pid,
    pid_organization_code,
    on_hand,
    unit_cost,
    eos_date,
    orderability,
    publish_date,
    creation_date,
    created_by,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cg1_xxcmf_sp_bts_extract_pid
),

final AS (
    SELECT
        pid,
        pid_organization_code,
        on_hand,
        unit_cost,
        eos_date,
        orderability,
        publish_date,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_xxcmf_sp_bts_extract_pid
)

SELECT * FROM final