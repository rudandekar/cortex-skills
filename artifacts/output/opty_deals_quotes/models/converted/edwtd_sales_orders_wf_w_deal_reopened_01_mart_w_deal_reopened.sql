{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_reopened', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_REOPENED',
        'target_table': 'W_DEAL_REOPENED',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.945967+00:00'
    }
) }}

WITH 

source_w_deal_reopened AS (
    SELECT
        bk_deal_id,
        bk_reopen_created_on_dtm,
        reopen_updated_on_dtm,
        reopen_reason_txt,
        source_updated_dtm,
        source_created_dtm,
        source_updated_by_user_id,
        source_created_by_user_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_reopened') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_reopen_created_on_dtm,
        reopen_updated_on_dtm,
        reopen_reason_txt,
        source_updated_dtm,
        source_created_dtm,
        source_updated_by_user_id,
        source_created_by_user_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_deal_reopened
)

SELECT * FROM final