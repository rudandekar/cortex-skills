{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_cust_spcfc_std_dscnt_req_lnk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CUST_SPCFC_STD_DSCNT_REQ_LNK',
        'target_table': 'W_CUST_SPCFC_STD_DSCNT_REQ_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.948707+00:00'
    }
) }}

WITH 

source_st_edms_analytic_attribs AS (
    SELECT
        request_num,
        request_ver,
        contract_number,
        discount_id,
        creation_date
    FROM {{ source('raw', 'st_edms_analytic_attribs') }}
),

transformed_exp_w_cust_spcfc_std_dscnt_req_lnk AS (
    SELECT
    request_num,
    request_ver,
    discount_id,
    creation_date,
    'E_SLSORD_BATCH' AS edw_create_user,
    'E_SLSORD_BATCH' AS edw_update_user,
    'I' AS action_code,
    '=' AS dml_type
    FROM source_st_edms_analytic_attribs
),

final AS (
    SELECT
        bk_discount_request_id_int,
        bk_discount_req_ver_num_int,
        bk_discount_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_w_cust_spcfc_std_dscnt_req_lnk
)

SELECT * FROM final