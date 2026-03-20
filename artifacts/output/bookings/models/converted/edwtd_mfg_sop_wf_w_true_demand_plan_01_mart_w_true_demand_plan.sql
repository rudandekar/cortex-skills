{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_true_demand_plan', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_W_TRUE_DEMAND_PLAN',
        'target_table': 'W_TRUE_DEMAND_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.167788+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_crp_true_demand AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        true_demand_plan,
        net_pos_plan,
        creation_date,
        id
    FROM {{ source('raw', 'st_xxcmf_sp_crp_true_demand') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_num_int,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        true_demand_plan_usd_amt,
        net_pos_plan_usd_amt,
        bk_plan_publish_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxcmf_sp_crp_true_demand
)

SELECT * FROM final