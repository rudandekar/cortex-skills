{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_line_as_quote_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_LINE_AS_QUOTE_SEP',
        'target_table': 'N_SALES_ORDER_LINE_AS_QUOTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.761829+00:00'
    }
) }}

WITH 

source_w_sales_order_line_as_quote_sep AS (
    SELECT
        sales_order_line_key,
        bk_as_quote_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_line_as_quote_sep') }}
),

final AS (
    SELECT
        sales_order_line_key,
        bk_as_quote_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd
    FROM source_w_sales_order_line_as_quote_sep
)

SELECT * FROM final