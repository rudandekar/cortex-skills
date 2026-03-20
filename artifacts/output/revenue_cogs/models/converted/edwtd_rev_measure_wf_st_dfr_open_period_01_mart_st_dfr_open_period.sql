{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dfr_open_period', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_DFR_OPEN_PERIOD',
        'target_table': 'ST_DFR_OPEN_PERIOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.468211+00:00'
    }
) }}

WITH 

source_st_ae_open_period_all AS (
    SELECT
        module_id,
        module_name,
        fiscal_month_id,
        open_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM {{ source('raw', 'st_ae_open_period_all') }}
),

final AS (
    SELECT
        fiscal_month_id,
        open_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        product_open_mth_flg,
        service_open_mth_flg
    FROM source_st_ae_open_period_all
)

SELECT * FROM final