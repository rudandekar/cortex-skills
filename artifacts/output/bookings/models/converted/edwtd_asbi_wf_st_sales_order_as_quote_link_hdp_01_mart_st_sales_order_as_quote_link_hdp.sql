{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_sales_order_as_quote_link_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_SALES_ORDER_AS_QUOTE_LINK_HDP',
        'target_table': 'ST_SALES_ORDER_AS_QUOTE_LINK_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.083392+00:00'
    }
) }}

WITH 

source_ff_sales_order_as_quote_link AS (
    SELECT
        sales_order_key,
        bk_as_quote_num,
        ss_cd
    FROM {{ source('raw', 'ff_sales_order_as_quote_link') }}
),

transformed_exptrans AS (
    SELECT
    sales_order_key,
    bk_as_quote_num,
    ss_cd,
    CURRENT_TIMESTAMP() AS createdtm
    FROM source_ff_sales_order_as_quote_link
),

final AS (
    SELECT
        sales_order_key,
        bk_as_quote_num,
        ss_cd,
        created_dtm
    FROM transformed_exptrans
)

SELECT * FROM final