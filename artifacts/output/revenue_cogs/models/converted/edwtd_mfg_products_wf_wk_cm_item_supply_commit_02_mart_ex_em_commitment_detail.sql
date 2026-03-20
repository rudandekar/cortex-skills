{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_cm_item_supply_commit', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_WK_CM_ITEM_SUPPLY_COMMIT',
        'target_table': 'EX_EM_COMMITMENT_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.198485+00:00'
    }
) }}

WITH 

source_st_em_commitment_detail AS (
    SELECT
        commitment_detail_id,
        commitment_id,
        period_start,
        period_end,
        commitment_qty,
        commit_message,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_em_commitment_detail') }}
),

source_st_em_commitment AS (
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
    FROM {{ source('raw', 'st_em_commitment') }}
),

final AS (
    SELECT
        product_id,
        partner_id,
        pip_partner_id,
        plan_date,
        creation_date,
        last_update,
        commitment_detail_id,
        commitment_id,
        period_start,
        period_end,
        commitment_qty,
        commit_message,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM source_st_em_commitment
)

SELECT * FROM final