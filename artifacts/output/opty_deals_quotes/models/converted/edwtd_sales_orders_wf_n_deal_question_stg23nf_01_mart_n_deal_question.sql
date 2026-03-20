{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_question_stg23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUESTION_STG23NF',
        'target_table': 'N_DEAL_QUESTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.872017+00:00'
    }
) }}

WITH 

source_n_deal_question AS (
    SELECT
        deal_question_key,
        deal_question_name,
        sk_question_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_code
    FROM {{ source('raw', 'n_deal_question') }}
),

source_sm_deal_question AS (
    SELECT
        deal_question_key,
        sk_question_id_int,
        edw_create_dtm,
        edw_create_user,
        ss_code
    FROM {{ source('raw', 'sm_deal_question') }}
),

final AS (
    SELECT
        deal_question_key,
        deal_question_name,
        sk_question_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_code
    FROM source_sm_deal_question
)

SELECT * FROM final