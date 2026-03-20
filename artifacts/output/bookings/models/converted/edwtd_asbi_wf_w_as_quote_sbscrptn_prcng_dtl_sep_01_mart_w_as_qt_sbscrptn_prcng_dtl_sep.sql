{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_quote_sbscrptn_prcng_dtl_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_AS_QUOTE_SBSCRPTN_PRCNG_DTL_SEP',
        'target_table': 'W_AS_QT_SBSCRPTN_PRCNG_DTL_SEP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.777546+00:00'
    }
) }}

WITH 

source_w_as_qt_sbscrptn_prcng_dtl_sep AS (
    SELECT
        bk_service_component_name,
        service_product_key,
        bk_service_program_name,
        bk_as_quote_num,
        service_product_price_usd_amt,
        sk_quote_component_id,
        source_deleted_flg,
        total_component_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_as_qt_sbscrptn_prcng_dtl_sep') }}
),

final AS (
    SELECT
        bk_service_component_name,
        service_product_key,
        bk_service_program_name,
        bk_as_quote_num,
        service_product_price_usd_amt,
        sk_quote_component_id,
        source_deleted_flg,
        total_component_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        action_code,
        dml_type
    FROM source_w_as_qt_sbscrptn_prcng_dtl_sep
)

SELECT * FROM final