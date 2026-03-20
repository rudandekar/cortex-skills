{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_question_response', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUESTION_RESPONSE',
        'target_table': 'N_DEAL_QUESTION_RESPONSE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.958083+00:00'
    }
) }}

WITH 

source_n_deal_question_response AS (
    SELECT
        deal_question_response_key,
        deal_question_response_name,
        deal_id,
        deal_question_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_deal_question_response') }}
),

final AS (
    SELECT
        deal_question_response_key,
        deal_question_response_name,
        deal_id,
        deal_question_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_deal_question_response
)

SELECT * FROM final