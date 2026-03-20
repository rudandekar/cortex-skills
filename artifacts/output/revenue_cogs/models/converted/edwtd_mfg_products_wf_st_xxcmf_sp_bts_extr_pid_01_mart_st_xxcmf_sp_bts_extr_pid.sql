{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_bts_extr_pid', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_BTS_EXTR_PID',
        'target_table': 'ST_XXCMF_SP_BTS_EXTR_PID',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.194865+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_bts_extr_pid AS (
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
    FROM {{ source('raw', 'ff_xxcmf_sp_bts_extr_pid') }}
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
    FROM source_ff_xxcmf_sp_bts_extr_pid
)

SELECT * FROM final