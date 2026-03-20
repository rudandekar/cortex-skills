{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_deal_quote_line_concession_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_DEAL_QUOTE_LINE_CONCESSION_RNWL',
        'target_table': 'SM_DEAL_QUOTE_LINE_CONCESSION_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.905220+00:00'
    }
) }}

WITH 

source_sm_deal_quote_line_concession_rnwl AS (
    SELECT
        bk_deal_qt_ln_concession_key,
        sk_object_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_deal_quote_line_concession_rnwl') }}
),

final AS (
    SELECT
        bk_deal_qt_ln_concession_key,
        sk_object_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_deal_quote_line_concession_rnwl
)

SELECT * FROM final