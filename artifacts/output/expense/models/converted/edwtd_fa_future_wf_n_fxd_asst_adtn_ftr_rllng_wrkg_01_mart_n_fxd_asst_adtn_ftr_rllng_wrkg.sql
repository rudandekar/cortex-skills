{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fxd_asst_adtn_ftr_rllng_wrkg', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_N_FXD_ASST_ADTN_FTR_RLLNG_WRKG',
        'target_table': 'N_FXD_ASST_ADTN_FTR_RLLNG_WRKG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.841892+00:00'
    }
) }}

WITH 

source_w_fxd_asst_adtn_ftr_rllng_wrkg AS (
    SELECT
        bk_capital_future_proj_id_int,
        bk_department_cd,
        bk_company_cd,
        bk_financial_account_cd,
        bk_cptl_future_fin_dtl_acct_cd,
        bk_rspnsbl_cptl_ftr_org_name,
        bk_planned_for_dt,
        bk_planned_for_period_type_cd,
        bk_fa_addition_future_type,
        rolling_working_opex_usd_amt,
        rllng_wrkng_asset_cost_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fxd_asst_adtn_ftr_rllng_wrkg') }}
),

final AS (
    SELECT
        bk_capital_future_proj_id_int,
        bk_department_cd,
        bk_company_cd,
        bk_financial_account_cd,
        bk_cptl_future_fin_dtl_acct_cd,
        bk_rspnsbl_cptl_ftr_org_name,
        bk_planned_for_dt,
        bk_planned_for_period_type_cd,
        bk_fa_addition_future_type,
        rolling_working_opex_usd_amt,
        rllng_wrkng_asset_cost_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_fxd_asst_adtn_ftr_rllng_wrkg
)

SELECT * FROM final