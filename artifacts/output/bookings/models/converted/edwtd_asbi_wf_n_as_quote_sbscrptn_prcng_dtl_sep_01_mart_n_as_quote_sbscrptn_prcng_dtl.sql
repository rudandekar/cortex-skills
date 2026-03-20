{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_quote_sbscrptn_prcng_dtl_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_QUOTE_SBSCRPTN_PRCNG_DTL_SEP',
        'target_table': 'N_AS_QUOTE_SBSCRPTN_PRCNG_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.553819+00:00'
    }
) }}

WITH 

source_n_as_quote_sbscrptn_prcng_dtl1 AS (
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
        bk_ss_cd
    FROM {{ source('raw', 'n_as_quote_sbscrptn_prcng_dtl1') }}
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
        bk_ss_cd
    FROM source_n_as_quote_sbscrptn_prcng_dtl1
)

SELECT * FROM final