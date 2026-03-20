{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_market_lob_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_MARKET_LOB_TV',
        'target_table': 'N_FINANCIAL_MARKET_LOB_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.928370+00:00'
    }
) }}

WITH 

source_n_financial_market_lob_tv AS (
    SELECT
        bk_financial_market_lob_id,
        start_tv_dt,
        end_tv_dt,
        financial_market_lob_descr,
        sk_market_lob_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'n_financial_market_lob_tv') }}
),

source_w_financial_market_lob AS (
    SELECT
        bk_financial_market_lob_id,
        start_tv_dt,
        end_tv_dt,
        financial_market_lob_descr,
        sk_market_lob_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_financial_market_lob') }}
),

final AS (
    SELECT
        bk_financial_market_lob_id,
        start_tv_dt,
        end_tv_dt,
        financial_market_lob_descr,
        sk_market_lob_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_w_financial_market_lob
)

SELECT * FROM final