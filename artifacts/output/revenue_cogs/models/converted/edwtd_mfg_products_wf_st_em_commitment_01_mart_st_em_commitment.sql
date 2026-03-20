{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_em_commitment', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_EM_COMMITMENT',
        'target_table': 'ST_EM_COMMITMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.386511+00:00'
    }
) }}

WITH 

source_ff_em_commitment AS (
    SELECT
        commitment_id,
        product_id,
        planning_division,
        plan_date,
        partner_id,
        pip_partner_id,
        product_id_type,
        last_update,
        creation_date,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_em_commitment') }}
),

final AS (
    SELECT
        commitment_id,
        product_id,
        planning_division,
        plan_date,
        partner_id,
        pip_partner_id,
        product_id_type,
        last_update,
        creation_date,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_em_commitment
)

SELECT * FROM final