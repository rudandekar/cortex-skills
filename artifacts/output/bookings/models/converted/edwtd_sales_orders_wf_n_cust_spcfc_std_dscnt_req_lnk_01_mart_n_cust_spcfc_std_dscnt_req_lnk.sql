{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cust_spcfc_std_dscnt_req_lnk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CUST_SPCFC_STD_DSCNT_REQ_LNK',
        'target_table': 'N_CUST_SPCFC_STD_DSCNT_REQ_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.604849+00:00'
    }
) }}

WITH 

source_w_cust_spcfc_std_dscnt_req_lnk AS (
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
    FROM {{ source('raw', 'w_cust_spcfc_std_dscnt_req_lnk') }}
),

final AS (
    SELECT
        bk_discount_request_id_int,
        bk_discount_req_ver_num_int,
        bk_discount_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_cust_spcfc_std_dscnt_req_lnk
)

SELECT * FROM final