{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_fixed_asset_addition_future', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FIXED_ASSET_ADDITION_FUTURE',
        'target_table': 'W_FIXED_ASSET_ADDITION_FUTURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.989936+00:00'
    }
) }}

WITH 

source_st_cap_roll_work_plan_arc AS (
    SELECT
        batch_id,
        project_id,
        amount,
        period_id,
        account_id,
        org_id,
        country_department,
        cip_flag,
        period_name,
        quarter_id,
        planning_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cap_roll_work_plan_arc') }}
),

final AS (
    SELECT
        bk_capital_future_proj_id_int,
        bk_department_code,
        bk_company_code,
        bk_financial_account_cd,
        bk_cptl_future_fin_dtl_acct_cd,
        bk_rspnsbl_cptl_ftr_org_name,
        bk_published_in_dt,
        bk_planned_for_dt,
        bk_planned_for_period_type_cd,
        bk_fa_addition_future_type,
        ru_fa_addtn_asset_cmmt_usd_amt,
        ru_fa_addtn_opex_cmmt_usd_amt,
        ru_fa_addtn_asset_bdgt_usd_amt,
        ru_fa_addtn_opex_bdgt_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_cap_roll_work_plan_arc
)

SELECT * FROM final