{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rae_adjustment_schedule_line', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_RAE_ADJUSTMENT_SCHEDULE_LINE',
        'target_table': 'N_RAE_ADJUSTMENT_SCHEDULE_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.644297+00:00'
    }
) }}

WITH 

source_w_rae_adjustment_schedule_line AS (
    SELECT
        rae_adjustment_key,
        rae_adjustment_schedule_ln_key,
        schedule_line_num_int,
        accounting_rule_type_cd,
        gl_dtm,
        catchup_flg,
        schedule_line_trnsctnl_amt,
        schedule_line_functional_amt,
        error_flg,
        gl_posted_role,
        credit_gl_account_key,
        debit_gl_account_key,
        transactional_currency_cd,
        operating_unit_name_cd,
        ru_credit_rae_adj_gl_dist_key,
        ru_debit_rae_adj_gl_dist_key,
        sk_schedule_line_id_int,
        ss_cd,
        adjustment_line_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sk_trx_id_int,
        offer_attribution_id_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rae_adjustment_schedule_line') }}
),

final AS (
    SELECT
        rae_adjustment_key,
        rae_adjustment_schedule_ln_key,
        schedule_line_num_int,
        accounting_rule_type_cd,
        gl_dtm,
        catchup_flg,
        schedule_line_trnsctnl_amt,
        schedule_line_functional_amt,
        error_flg,
        gl_posted_role,
        credit_gl_account_key,
        debit_gl_account_key,
        transactional_currency_cd,
        operating_unit_name_cd,
        ru_credit_rae_adj_gl_dist_key,
        ru_debit_rae_adj_gl_dist_key,
        sk_schedule_line_id_int,
        ss_cd,
        adjustment_line_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sk_trx_id_int,
        offer_attribution_id_int
    FROM source_w_rae_adjustment_schedule_line
)

SELECT * FROM final