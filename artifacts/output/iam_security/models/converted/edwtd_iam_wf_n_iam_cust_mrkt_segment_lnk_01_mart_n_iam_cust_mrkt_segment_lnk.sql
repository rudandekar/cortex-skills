{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_cust_mrkt_segment_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_CUST_MRKT_SEGMENT_LNK',
        'target_table': 'N_IAM_CUST_MRKT_SEGMENT_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.762059+00:00'
    }
) }}

WITH 

source_w_iam_cust_mrkt_segment_lnk AS (
    SELECT
        iam_role_name,
        iam_user_key,
        iam_application_key,
        bk_customer_market_segment_cd,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_cust_mrkt_segment_lnk') }}
),

final AS (
    SELECT
        iam_role_name,
        iam_user_key,
        iam_application_key,
        bk_customer_market_segment_cd,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_iam_cust_mrkt_segment_lnk
)

SELECT * FROM final