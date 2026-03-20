{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_purchase_reqn_line_distrib', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PURCHASE_REQN_LINE_DISTRIB',
        'target_table': 'N_PURCHASE_REQN_LINE_DISTRIB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.928121+00:00'
    }
) }}

WITH 

source_w_purchase_reqn_line_distrib AS (
    SELECT
        purchase_reqn_line_distrib_key,
        bk_pr_line_distrib_num_int,
        purchase_requisition_line_key,
        expense_or_asset_gl_acct_key,
        source_deleted_flg,
        offset_gla_company_cd,
        offset_gla_financial_locn_cd,
        offset_gla_financial_acct_cd,
        offset_gla_project_cd,
        offset_gla_subaccount_cd,
        offset_gla_department_cd,
        cip_num,
        sk_req_distrib_id_int,
        ss_cd,
        bk_original_dept_cd,
        bk_original_company_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_purchase_reqn_line_distrib') }}
),

final AS (
    SELECT
        purchase_reqn_line_distrib_key,
        bk_pr_line_distrib_num_int,
        purchase_requisition_line_key,
        expense_or_asset_gl_acct_key,
        source_deleted_flg,
        offset_gla_company_cd,
        offset_gla_financial_locn_cd,
        offset_gla_financial_acct_cd,
        offset_gla_project_cd,
        offset_gla_subaccount_cd,
        offset_gla_department_cd,
        cip_num,
        sk_req_distrib_id_int,
        ss_cd,
        bk_original_dept_cd,
        bk_original_company_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_purchase_reqn_line_distrib
)

SELECT * FROM final