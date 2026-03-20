{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rec_rev_sys_upd_xaas', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REC_REV_SYS_UPD_XAAS',
        'target_table': 'WI_GL_REC_REV_SYS_UPD_XAAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.667032+00:00'
    }
) }}

WITH 

source_wi_gl_rec_rev_sys_upd_xaas AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_fiscal_year_mth_number_int,
        sales_territory_key,
        sales_rep_number,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        contract_start_date,
        contract_end_date,
        contract_duration,
        dv_contract_duration,
        revenue_classification,
        recurring_flg,
        dv_rev_class_rule_name
    FROM {{ source('raw', 'wi_gl_rec_rev_sys_upd_xaas') }}
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
        contract_start_date,
        contract_end_date,
        contract_duration,
        dv_contract_duration,
        revenue_classification,
        recurring_flg,
        dv_rev_class_rule_name
    FROM source_wi_gl_rec_rev_sys_upd_xaas
)

SELECT * FROM final