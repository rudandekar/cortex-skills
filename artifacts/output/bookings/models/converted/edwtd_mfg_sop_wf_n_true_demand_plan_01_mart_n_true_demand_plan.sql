{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_true_demand_plan', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_N_TRUE_DEMAND_PLAN',
        'target_table': 'N_TRUE_DEMAND_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.393859+00:00'
    }
) }}

WITH 

source_w_true_demand_plan AS (
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
    FROM {{ source('raw', 'w_true_demand_plan') }}
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
        edw_update_user
    FROM source_w_true_demand_plan
)

SELECT * FROM final