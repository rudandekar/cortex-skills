{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iso_currency_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_ISO_CURRENCY_TV',
        'target_table': 'N_ISO_CURRENCY_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.832471+00:00'
    }
) }}

WITH 

source_w_iso_currency AS (
    SELECT
        bk_iso_currency_code,
        start_tv_date,
        end_tv_date,
        iso_currency_name,
        sk_currency_code,
        ss_code,
        iso_currency_enabled_flag,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iso_currency') }}
),

final AS (
    SELECT
        bk_iso_currency_code,
        start_tv_date,
        end_tv_date,
        iso_currency_name,
        sk_currency_code,
        ss_code,
        iso_currency_enabled_flag,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user
    FROM source_w_iso_currency
)

SELECT * FROM final