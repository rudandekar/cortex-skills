{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_em_commitment', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_EM_COMMITMENT',
        'target_table': 'FF_EM_COMMITMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.121560+00:00'
    }
) }}

WITH 

source_em_commitment AS (
    SELECT
        commitment_id,
        product_id,
        planning_division,
        plan_date,
        partner_id,
        pip_partner_id,
        product_id_type,
        last_update,
        creation_date
    FROM {{ source('raw', 'em_commitment') }}
),

transformed_exp_em_commitment AS (
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
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_em_commitment
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
    FROM transformed_exp_em_commitment
)

SELECT * FROM final