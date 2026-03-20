{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_as_quote_link_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_AS_QUOTE_LINK_SEP',
        'target_table': 'W_SALES_ORDR_AS_QT_LNK_SEP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.579940+00:00'
    }
) }}

WITH 

source_w_sales_ordr_as_qt_lnk_sep AS (
    SELECT
        sales_order_key,
        bk_as_quote_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_ordr_as_qt_lnk_sep') }}
),

final AS (
    SELECT
        sales_order_key,
        bk_as_quote_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        action_code,
        dml_type
    FROM source_w_sales_ordr_as_qt_lnk_sep
)

SELECT * FROM final