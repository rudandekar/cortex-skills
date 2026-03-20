{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_question_response_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_QUESTION_RESPONSE_RNWL',
        'target_table': 'EX_CQ_QF_RESPONSE_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.873083+00:00'
    }
) }}

WITH 

source_ex_cq_qf_response_rnwl AS (
    SELECT
        batch_id,
        deal_object_id,
        question_id,
        value_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ex_cq_qf_response_rnwl') }}
),

source_sm_deal_question_response1 AS (
    SELECT
        deal_question_response_key,
        deal_question_response_name,
        bk_deal_id,
        sk_question_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_deal_question_response1') }}
),

source_w_deal_question_response_rnwl AS (
    SELECT
        deal_question_response_key,
        deal_question_response_name,
        deal_id,
        deal_question_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_question_response_rnwl') }}
),

final AS (
    SELECT
        batch_id,
        deal_object_id,
        question_id,
        value_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code
    FROM source_w_deal_question_response_rnwl
)

SELECT * FROM final