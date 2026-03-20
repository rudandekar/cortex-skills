{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cust_spcfc_and_std_dscnt_req', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CUST_SPCFC_AND_STD_DSCNT_REQ',
        'target_table': 'N_CUST_SPCFC_AND_STD_DSCNT_REQ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.448113+00:00'
    }
) }}

WITH 

source_w_cust_spcfc_and_std_dscnt_req AS (
    SELECT
        bk_discount_request_id_int,
        bk_discount_req_ver_num_int,
        contract_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_cust_spcfc_and_std_dscnt_req') }}
),

final AS (
    SELECT
        bk_discount_request_id_int,
        bk_discount_req_ver_num_int,
        contract_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_cust_spcfc_and_std_dscnt_req
)

SELECT * FROM final