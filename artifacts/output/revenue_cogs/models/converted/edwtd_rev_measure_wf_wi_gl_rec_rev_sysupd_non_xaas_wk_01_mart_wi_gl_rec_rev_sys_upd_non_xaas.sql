{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rec_rev_sysupd_non_xaas_wk', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REC_REV_SYSUPD_NON_XAAS_WK',
        'target_table': 'WI_GL_REC_REV_SYS_UPD_NON_XAAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.857046+00:00'
    }
) }}

WITH 

source_wi_gl_rec_rev_sys_upd_non_xaas AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_fiscal_year_mth_number_int,
        sales_territory_key,
        sales_rep_number,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        ru_srvc_cntrct_drtn_mnths_cnt,
        ru_rev_trnsfr_cntrct_start_dt,
        ru_rev_trnsfr_cntrct_end_dt,
        ru_accounting_rule_start_date,
        accounting_rule_end_dt,
        ru_acctg_rule_duration_mth_cnt,
        contract_start_date,
        contract_end_date,
        contract_duration,
        dv_contract_duration,
        revenue_classification,
        recurring_flg,
        dv_rev_class_rule_name
    FROM {{ source('raw', 'wi_gl_rec_rev_sys_upd_non_xaas') }}
),

final AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_fiscal_year_mth_number_int,
        sales_territory_key,
        sales_rep_number,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        ru_srvc_cntrct_drtn_mnths_cnt,
        ru_rev_trnsfr_cntrct_start_dt,
        ru_rev_trnsfr_cntrct_end_dt,
        ru_accounting_rule_start_date,
        accounting_rule_end_dt,
        ru_acctg_rule_duration_mth_cnt,
        contract_start_date,
        contract_end_date,
        contract_duration,
        dv_contract_duration,
        revenue_classification,
        recurring_flg,
        dv_rev_class_rule_name
    FROM source_wi_gl_rec_rev_sys_upd_non_xaas
)

SELECT * FROM final