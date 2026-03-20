{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_line_as_quote_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_LINE_AS_QUOTE_SEP',
        'target_table': 'W_SALES_ORDR_LN_AS_QT_SEP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.984393+00:00'
    }
) }}

WITH 

source_w_sales_ordr_ln_as_qt_sep AS (
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
    FROM {{ source('raw', 'w_sales_ordr_ln_as_qt_sep') }}
),

final AS (
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
    FROM source_w_sales_ordr_ln_as_qt_sep
)

SELECT * FROM final