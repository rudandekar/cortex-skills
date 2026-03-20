{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcmf_sp_bts_extr_tan', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_XXCMF_SP_BTS_EXTR_TAN',
        'target_table': 'FF_XXCMF_SP_BTS_EXTR_TAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.857541+00:00'
    }
) }}

WITH 

source_cg1_xxcmf_sp_bts_extract_tan AS (
    SELECT
        tan,
        tan_organization_code,
        on_hand,
        unit_cost,
        publish_date,
        creation_date,
        created_by,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_xxcmf_sp_bts_extract_tan') }}
),

transformed_exp_cg1_xxcmf_sp_bts_extract_tan AS (
    SELECT
    tan,
    tan_organization_code,
    on_hand,
    unit_cost,
    publish_date,
    creation_date,
    created_by,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cg1_xxcmf_sp_bts_extract_tan
),

final AS (
    SELECT
        tan,
        tan_organization_code,
        on_hand,
        unit_cost,
        publish_date,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_cg1_xxcmf_sp_bts_extract_tan
)

SELECT * FROM final