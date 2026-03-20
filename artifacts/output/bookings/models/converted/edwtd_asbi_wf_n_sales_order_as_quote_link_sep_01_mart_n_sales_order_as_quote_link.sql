{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_as_quote_link_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_AS_QUOTE_LINK_SEP',
        'target_table': 'N_SALES_ORDER_AS_QUOTE_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.660011+00:00'
    }
) }}

WITH 

source_n_sales_order_as_quote_link AS (
    SELECT
        sales_order_key,
        bk_as_quote_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd
    FROM {{ source('raw', 'n_sales_order_as_quote_link') }}
),

final AS (
    SELECT
        sales_order_key,
        bk_as_quote_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd
    FROM source_n_sales_order_as_quote_link
)

SELECT * FROM final