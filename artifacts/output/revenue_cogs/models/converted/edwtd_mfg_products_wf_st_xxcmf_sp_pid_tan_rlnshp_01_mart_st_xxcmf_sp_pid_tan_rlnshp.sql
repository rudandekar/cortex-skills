{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_pid_tan_rlnshp', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_PID_TAN_RLNSHP',
        'target_table': 'ST_XXCMF_SP_PID_TAN_RLNSHP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.518399+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_pid_tan_rlnshp AS (
    SELECT
        pid,
        tan,
        description,
        site,
        effective_in,
        effective_out,
        quantity_per,
        supply_planner,
        mfg_planner,
        buyer_code,
        prod_family,
        business_unit,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcmf_sp_pid_tan_rlnshp') }}
),

final AS (
    SELECT
        pid,
        tan_id,
        description,
        site,
        effective_in,
        effective_out,
        quantity_per,
        supply_planner,
        mfg_planner,
        buyer_code,
        prod_family,
        business_unit,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_xxcmf_sp_pid_tan_rlnshp
)

SELECT * FROM final