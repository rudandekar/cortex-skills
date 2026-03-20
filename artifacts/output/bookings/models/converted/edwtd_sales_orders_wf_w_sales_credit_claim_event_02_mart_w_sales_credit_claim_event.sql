{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_credit_claim_event', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_CREDIT_CLAIM_EVENT',
        'target_table': 'W_SALES_CREDIT_CLAIM_EVENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.606288+00:00'
    }
) }}

WITH 

source_ex_otm_gct_events AS (
    SELECT
        event_id,
        claim_id,
        created_by,
        created_date,
        event_description,
        exception_type,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ex_otm_gct_events') }}
),

source_st_otm_gct_events AS (
    SELECT
        event_id,
        claim_id,
        created_by,
        created_date,
        event_description,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_otm_gct_events') }}
),

final AS (
    SELECT
        sales_credit_claim_event_key,
        claim_event_done_by_user_id,
        bk_sales_credit_claim_id_int,
        claim_event_dtm,
        claim_event_type_descr,
        sk_event_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_otm_gct_events
)

SELECT * FROM final