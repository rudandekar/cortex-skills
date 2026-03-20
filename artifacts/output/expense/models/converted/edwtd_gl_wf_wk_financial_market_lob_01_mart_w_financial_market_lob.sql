{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_market_lob', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_MARKET_LOB',
        'target_table': 'W_FINANCIAL_MARKET_LOB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.969612+00:00'
    }
) }}

WITH 

source_st_si_market_lob AS (
    SELECT
        batch_id,
        market_lob_id,
        market_lob_value,
        market_lob_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_market_lob') }}
),

transformed_exp_financial_market_lob AS (
    SELECT
    bk_financial_market_lob_id,
    start_tv_dt,
    end_tv_dt,
    financial_market_lob_descr,
    sk_market_lob_id_int,
    action_code,
    rank_index,
    dml_type
    FROM source_st_si_market_lob
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
        edw_update_dtm,
        action_code,
        dml_type
    FROM transformed_exp_financial_market_lob
)

SELECT * FROM final