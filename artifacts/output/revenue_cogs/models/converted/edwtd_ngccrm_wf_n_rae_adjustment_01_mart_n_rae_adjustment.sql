{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rae_adjustment', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_RAE_ADJUSTMENT',
        'target_table': 'N_RAE_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.562648+00:00'
    }
) }}

WITH 

source_w_rae_adjustment AS (
    SELECT
        rae_adjustment_key,
        rae_adjustment_category_cd,
        adjustment_descr,
        association_type,
        adjustment_status_cd,
        rprtd_adjust_value_func_amt,
        execution_event_cd,
        adjustment_source_cd,
        journal_category_cd,
        journal_source_name,
        variable_role,
        debit_gl_account_key,
        credit_gl_account_key,
        from_currency_cd,
        to_currency_cd,
        conversion_dt,
        operating_unit_name_cd,
        sk_rae_adjustment_id_int,
        ss_cd,
        ru_sales_order_line_key,
        ru_ar_trx_line_key,
        ru_adjustment_pct,
        ru_adjustment_start_dt,
        ru_adjustment_end_dt,
        ru_adjustment_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        adjustment_type_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rae_adjustment') }}
),

final AS (
    SELECT
        rae_adjustment_key,
        rae_adjustment_category_cd,
        adjustment_descr,
        association_type,
        adjustment_status_cd,
        rprtd_adjust_value_func_amt,
        execution_event_cd,
        adjustment_source_cd,
        journal_category_cd,
        journal_source_name,
        variable_role,
        debit_gl_account_key,
        credit_gl_account_key,
        from_currency_cd,
        to_currency_cd,
        conversion_dt,
        operating_unit_name_cd,
        sk_rae_adjustment_id_int,
        ss_cd,
        ru_sales_order_line_key,
        ru_ar_trx_line_key,
        ru_adjustment_pct,
        ru_adjustment_start_dt,
        ru_adjustment_end_dt,
        ru_adjustment_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        adjustment_type_cd
    FROM source_w_rae_adjustment
)

SELECT * FROM final