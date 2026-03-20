{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_theater_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_THEATER_TV',
        'target_table': 'N_FINANCIAL_THEATER_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.978828+00:00'
    }
) }}

WITH 

source_w_financial_theater AS (
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
    FROM {{ source('raw', 'w_financial_theater') }}
),

transformed_exptrans AS (
    SELECT
    bk_financial_theater_name_code,
    start_tv_date,
    end_tv_date,
    sk_theater_id_int,
    action_code,
    dml_type
    FROM source_w_financial_theater
),

final AS (
    SELECT
        bk_financial_theater_name_code,
        start_tv_date,
        end_tv_date,
        sk_theater_id_int
    FROM transformed_exptrans
)

SELECT * FROM final