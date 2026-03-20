{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_quote_comment', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_QUOTE_COMMENT',
        'target_table': 'N_QUOTE_COMMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.954382+00:00'
    }
) }}

WITH 

source_w_quote_comment AS (
    SELECT
        comment_id_int,
        bk_quote_num,
        comment_created_by_cisco_worker_party_key,
        bk_deal_id,
        ss_code,
        comment_created_date,
        comment_created_by_id,
        rejection_reason_flag,
        comment_text,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_quote_comment') }}
),

final AS (
    SELECT
        comment_id_int,
        bk_quote_num,
        comment_created_by_cisco_worker_party_key,
        bk_deal_id,
        ss_code,
        comment_created_date,
        comment_created_by_id,
        rejection_reason_flag,
        comment_text,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_quote_comment
)

SELECT * FROM final