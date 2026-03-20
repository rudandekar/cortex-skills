{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_apsp_bcbd_erp_sls_rep_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_APSP_BCBD_ERP_SLS_REP_LNK',
        'target_table': 'N_APSP_BCBD_ERP_SLS_REP_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.726394+00:00'
    }
) }}

WITH 

source_w_apsp_bcbd_erp_sls_rep_lnk AS (
    SELECT
        bcbd_user_key,
        bk_iam_role_name,
        iam_application_key,
        bk_sales_rep_num,
        bk_fiscal_year_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_bcbd_erp_sls_rep_lnk') }}
),

final AS (
    SELECT
        bcbd_user_key,
        bk_iam_role_name,
        iam_application_key,
        bk_sales_rep_num,
        bk_fiscal_year_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_apsp_bcbd_erp_sls_rep_lnk
)

SELECT * FROM final