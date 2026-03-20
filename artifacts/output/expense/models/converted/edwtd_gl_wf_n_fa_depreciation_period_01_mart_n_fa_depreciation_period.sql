{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fa_depreciation_period', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FA_DEPRECIATION_PERIOD',
        'target_table': 'N_FA_DEPRECIATION_PERIOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.957823+00:00'
    }
) }}

WITH 

source_w_fa_depreciation_period AS (
    SELECT
        bk_fa_book_type_cd,
        bk_dprctn_period_seq_num_int,
        sk_period_counter_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        period_actual_open_dtm,
        period_actual_closed_dtm,
        dv_fiscal_year_month_int
    FROM {{ source('raw', 'w_fa_depreciation_period') }}
),

final AS (
    SELECT
        bk_fa_book_type_cd,
        bk_dprctn_period_seq_num_int,
        sk_period_counter_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        period_actual_open_dtm,
        period_actual_closed_dtm,
        dv_fiscal_year_month_int
    FROM source_w_fa_depreciation_period
)

SELECT * FROM final