{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_currency_country_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_CURRENCY_COUNTRY_TV',
        'target_table': 'N_CURRENCY_COUNTRY_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.145117+00:00'
    }
) }}

WITH 

source_w_currency_country AS (
    SELECT
        bk_iso_country_code,
        bk_iso_currency_code,
        start_tv_date,
        end_tv_date,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_currency_country') }}
),

final AS (
    SELECT
        bk_iso_country_code,
        bk_iso_currency_code,
        start_tv_date,
        end_tv_date,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user
    FROM source_w_currency_country
)

SELECT * FROM final