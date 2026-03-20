{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_iso_currency', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_ISO_CURRENCY',
        'target_table': 'W_ISO_CURRENCY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.153632+00:00'
    }
) }}

WITH 

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

source_st_mf_fnd_currencies_tl AS (
    SELECT
        batch_id,
        currency_code,
        global_name,
        language_code,
        description,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fnd_currencies_tl') }}
),

source_st_mf_fnd_currencies AS (
    SELECT
        batch_id,
        currency_code,
        global_name,
        issuing_territory_code,
        enabled_flag,
        iso_flag,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fnd_currencies') }}
),

transformed_exp_w_iso_currency AS (
    SELECT
    batch_id,
    currency_code,
    start_tv_date,
    end_tv_date,
    description,
    source_system_code,
    enabled_flag,
    create_datetime,
    action_code,
    rank_index,
    dml_type
    FROM source_st_mf_fnd_currencies
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
        edw_create_user,
        action_code,
        dml_type
    FROM transformed_exp_w_iso_currency
)

SELECT * FROM final