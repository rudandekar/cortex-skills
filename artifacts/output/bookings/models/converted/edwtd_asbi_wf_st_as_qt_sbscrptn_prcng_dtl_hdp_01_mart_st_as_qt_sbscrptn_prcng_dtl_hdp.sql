{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_qt_sbscrptn_prcng_dtl_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QT_SBSCRPTN_PRCNG_DTL_HDP',
        'target_table': 'ST_AS_QT_SBSCRPTN_PRCNG_DTL_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.249375+00:00'
    }
) }}

WITH 

source_ff_as_qt_sbscrptn_prcng_dtl_hdp AS (
    SELECT
        bk_service_component_name,
        service_product_key,
        bk_service_program_name,
        bk_as_quote_num,
        service_product_price_usd_amt,
        sk_quote_component_id,
        source_deleted_flg,
        total_component_qty,
        ss_cd
    FROM {{ source('raw', 'ff_as_qt_sbscrptn_prcng_dtl_hdp') }}
),

transformed_exptrans1 AS (
    SELECT
    bk_service_component_name,
    service_product_key,
    bk_service_program_name,
    bk_as_quote_num,
    service_product_price_usd_amt,
    sk_quote_component_id,
    source_deleted_flg,
    total_component_qty,
    ss_cd,
    CURRENT_TIMESTAMP() AS created_dtm
    FROM source_ff_as_qt_sbscrptn_prcng_dtl_hdp
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
        bk_ss_cd,
        created_dtm
    FROM transformed_exptrans1
)

SELECT * FROM final