{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_theater', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_THEATER',
        'target_table': 'W_FINANCIAL_THEATER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.089982+00:00'
    }
) }}

WITH 

source_st_si_theater AS (
    SELECT
        batch_id,
        theater_id,
        theater_name,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_theater') }}
),

transformed_exp_w_financial_theater AS (
    SELECT
    batch_id,
    bk_financial_theater_name_code,
    start_date_active,
    sk_theater_id_int,
    end_date_active,
    action_code,
    rank_index,
    dml_type
    FROM source_st_si_theater
),

final AS (
    SELECT
        bk_financial_theater_name_code,
        start_tv_date,
        end_tv_date,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_theater_id_int,
        action_code,
        dml_type
    FROM transformed_exp_w_financial_theater
)

SELECT * FROM final