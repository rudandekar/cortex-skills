{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cisco_wrkr_pty_phone_expense', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_CISCO_WRKR_PTY_PHONE_EXPENSE',
        'target_table': 'N_CISCO_WRKR_PTY_PHONE_EXPENSE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.125715+00:00'
    }
) }}

WITH 

source_w_cisco_wrkr_pty_phone_expense AS (
    SELECT
        bk_mobile_telephone_num,
        bk_billing_cycle_start_dt,
        bk_mobile_carrier_name,
        bk_bill_to_dept_cd,
        bk_bill_to_company_cd,
        bk_mobile_plan_type_cd,
        cisco_worker_party_key,
        billed_usd_amt,
        source_refresh_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_cisco_wrkr_pty_phone_expense') }}
),

final AS (
    SELECT
        bk_mobile_telephone_num,
        bk_billing_cycle_start_dt,
        bk_mobile_carrier_name,
        bk_bill_to_dept_cd,
        bk_bill_to_company_cd,
        bk_mobile_plan_type_cd,
        cisco_worker_party_key,
        billed_usd_amt,
        source_refresh_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_cisco_wrkr_pty_phone_expense
)

SELECT * FROM final